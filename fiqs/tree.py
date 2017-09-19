# -*- coding: utf-8 -*-

RESERVED_KEYS = [
    'key', 'key_as_string',
    'doc_count',
    'from', 'from_as_string',
    'to', 'to_as_string',
]


class ResultTree(object):
    def __init__(self, es_result):
        if isinstance(es_result, dict):
            self.es_result = es_result
        elif hasattr(es_result, '_d_'):
            self.es_result = es_result._d_
        else:
            raise Exception('ResultTree expects a dict or an elasticsearch_dsl Response object')

    def flatten_result(self, **kwargs):
        if 'aggregations' not in self.es_result:
            return []

        self.add_others_line = kwargs.get('add_others_line', False)
        self.remove_nested_aggregations = kwargs.get('remove_nested_aggregations', True)

        aggregations = self.es_result['aggregations']
        return self._extract_lines(aggregations)

    def _is_nested_node(self, node, parent_is_root=True, same_level_keys=None):
        # Not even a node, or a list of buckets
        if not isinstance(node, dict):
            return False

        # Standard aggregation
        if 'buckets' in node:
            return False

        # Bucket
        if 'key' in node:
            return False

        # Range bucket
        if 'from' in node or 'to' in node:
            return False

        # Nested nodes have a doc_count
        if 'doc_count' not in node:
            return False

        # Can happen with filters aggregations
        if same_level_keys is not None:
            if not parent_is_root and 'doc_count' not in same_level_keys:
                return False

        dict_child_nodes = [
            child_node for child_node in node.values()
            if isinstance(child_node, dict)
        ]
        child_keys = node.keys()
        for child_node in dict_child_nodes:
            is_nested_child_node = self._is_nested_node(
                child_node,
                parent_is_root=False,
                same_level_keys=child_keys,
            )
            if 'doc_count' in child_node and not is_nested_child_node:
                return False

        # Node like {'value': 123.456}
        if all([not isinstance(child_node, dict) for child_node in node.values()]):
            return False

        return True

    def _remove_nested_aggregations(self, node, parent_is_root=True):
        while True:
            new_node = self.__remove_nested_aggregations(
                node,
                parent_is_root,
            )

            if new_node == node:
                break
            else:
                node = new_node

        return new_node

    def __remove_nested_aggregations(self, node, parent_is_root):
        _node = {}

        # We force an ordering to have a deterministic result
        child_keys = sorted(node.keys(), reverse=True)
        for key in child_keys:
            child_node = node[key]

            if key.startswith('reverse_nested'):
                _node[key] = child_node

            elif isinstance(child_node, dict):
                if self._is_nested_node(child_node, parent_is_root, child_keys):
                    _node.update(self._remove_nested_aggregations(
                        child_node,
                        parent_is_root=False,
                    ))
                else:
                    _node[key] = self._remove_nested_aggregations(
                        child_node,
                        parent_is_root=False,
                    )

            elif isinstance(child_node, list):
                _node[key] = [
                    self._remove_nested_aggregations(
                        gchild_node,
                        parent_is_root=False,
                    )
                    if isinstance(gchild_node, dict) else gchild_node
                    for gchild_node in child_node
                ]

            else:
                _node[key] = child_node

        return _node

    def _create_line(self, base_line, node):
        new_line = base_line.copy()

        for k, v in node.items():
            if k.startswith('reverse_nested'):
                for nested_k, nested_v in v.items():
                    if isinstance(nested_v, dict):
                        value = nested_v['value']
                    else:
                        value = nested_v
                    new_line['{}__{}'.format(k, nested_k)] = value

            elif k == 'doc_count':
                new_line[k] = v
            elif k in RESERVED_KEYS:
                continue
            elif 'value' in v:
                new_line[k] = v['value']

        return new_line

    def _create_others_line(self, base_line, key, others_doc_count):
        new_line = base_line.copy()

        new_line[key] = u'others'
        new_line[u'doc_count'] = others_doc_count

        return new_line

    def _is_leaf(self, node):
        # If there are still buckets, we are not on a leaf
        return 'buckets' not in node

    def _find_deeper_path(self, node):
        # The path should always end right before a buckets node, or lead to a leaf
        path = []
        current_key = None

        path.append('buckets')
        buckets = node['buckets']

        if isinstance(buckets, list):
            path.append(0)
            next_node = buckets[0]
        elif isinstance(buckets, dict):
            first_key = sorted(buckets.keys())[0]
            path.append(first_key)
            next_node = buckets[first_key]

        # We find the next key if there is one
        next_key = [k for k in next_node.keys() if k not in RESERVED_KEYS]
        if next_key:
            next_key = next_key[0]
            if 'buckets' in next_node[next_key]:
                path.append(next_key)
                current_key = next_key

        return path, current_key

    def _bootstrap_current_key(self, aggregations):
        return sorted([
            k for k in aggregations.keys()
            if k not in RESERVED_KEYS
        ])[0]

    def _extract_lines(self, aggregations):
        # Initialization
        lines = []
        depth = 0
        base_line = {}

        current_key = self._bootstrap_current_key(aggregations)
        node = aggregations[current_key]

        # Are we dealing with a metric without aggs?
        if 'buckets' not in node and 'doc_count' not in node:
            return [{
                key: aggregations[key]['value']
                for key in aggregations.keys()
            }]

        if self.remove_nested_aggregations:
            # We remove nested aggregations, I don't see the point
            # of exposing them and they are annoying to deal with
            aggregations = self._remove_nested_aggregations(aggregations)

        current_key = self._bootstrap_current_key(aggregations)
        path = [current_key]
        node = aggregations[current_key]

        while True:
            # We get the current node using the path
            node = aggregations

            for key in path:
                node = node[key]

            if self._is_leaf(node):
                # We create a new line:
                new_line = self._create_line(base_line, node)
                lines.append(new_line)

                # We delete the leaf
                parent = aggregations
                for key in path[:-1]:
                    parent = parent[key]

                del parent[path[-1]]

                # We update the path
                path.pop()
                path.pop()
                depth -= 1

                continue

            if self.add_others_line and 'sum_other_doc_count' in node:
                others_doc_count = node.pop('sum_other_doc_count')
                others_line = self._create_others_line(base_line, current_key, others_doc_count)
                lines.append(others_line)

            buckets = node['buckets']

            # If there are no more buckets, and we are at depth 0
            if not buckets and depth == 0:
                # If there is another level 0 aggregation, we work on it
                next_key = [
                    k for k in aggregations.keys()
                    if k not in RESERVED_KEYS and k != current_key
                ]
                if next_key:
                    aggregations.pop(current_key)
                    base_line.pop(current_key)
                    current_key = next_key[0]
                    path = [current_key]
                    continue

                # Otherwise we're done!
                break

            # If there are no more buckets but we're not at depth 0,
            # either there is another aggregation at our depth or we go higher
            if not buckets:
                base_line.pop(current_key, None)  # Buckets may have been empty from the start

                parent_bucket = aggregations
                for key in path[:-2]:
                    parent_bucket = parent_bucket[key]

                # Is there another aggregation at our level?
                next_key = [
                    k for k in parent_bucket[path[-2]].keys()
                    if k not in RESERVED_KEYS and k != current_key
                ]
                if not next_key:
                    # No, we delete whole bucket
                    del parent_bucket[path[-2]]

                    # We update the path, the depth and the current_key
                    path.pop()  # current_key
                    path.pop()  # `0` or first_key
                    path.pop()  # `buckets`
                    depth -= 1
                    current_key = path[-1]
                else:
                    # Yes, we only delete the current bucket
                    parent_bucket[path[-2]].pop(current_key)

                    # We update the path and the current_key
                    path.pop()  # current_key
                    current_key = next_key[0]
                    path.append(current_key)

                # We go again
                continue

            # We need to go one level deeper
            if isinstance(buckets, list):
                bucket = buckets[0]
                base_line.update({
                    current_key: bucket['key'],
                })
            elif isinstance(buckets, dict):
                first_key = sorted(buckets.keys())[0]
                bucket = buckets[first_key]
                base_line.update({
                    current_key: first_key,
                })

            added_path, next_key = self._find_deeper_path(node)
            path += added_path
            if next_key:
                current_key = next_key
            depth += 1

        return lines
