# -*- coding: utf-8 -*-

import copy

from elasticsearch_dsl.result import Response


class ResultTree(object):
    def __init__(self, es_result):
        if isinstance(es_result, Response):
            self.es_result = copy.deepcopy(es_result._d_)
        elif isinstance(es_result, dict):
            self.es_result = copy.deepcopy(es_result)
        else:
            raise Exception('ResultTree expects a dict or an elasticsearch_dsl Response object')

    def flatten_result(self):
        if 'aggregations' not in self.es_result:
            return []

        aggregations = self.es_result['aggregations']
        return self.extract_lines(aggregations)

    def create_line(self, base_line, node):
        new_line = base_line.copy()

        for k, v in node.iteritems():
            if k in ['key', 'key_as_string']:
                continue
            if k == 'doc_count':
                new_line[k] = v
            else:
                new_line[k] = v['value']

        return new_line

    def extract_lines(self, aggregations):
        lines = []
        depth = 0
        base_line = {}
        current_key = sorted(aggregations.keys())[0]
        path = [current_key]

        while True:
            # We get the current node using the path
            node = aggregations
            for key in path:
                node = node[key]

            # If there are no more buckets, we arrived on a leaf
            if 'buckets' not in node:
                # We create a new line:
                new_line = self.create_line(base_line, node)
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

            buckets = node['buckets']

            # If there are no more buckets, and we are at depth 0, we're done!
            if not buckets and depth == 0:
                break

            # If there are no more buckets but we're not at depth 0, we need to go one level higher
            if not buckets:
                # We delete the empty leaf
                parent_bucket = aggregations
                for key in path[:-2]:
                    parent_bucket = parent_bucket[key]

                del parent_bucket[path[-2]]

                # update the path and the depth
                depth -= 1
                path.pop()  # `0` or first_key
                path.pop()  # `buckets`
                path.pop()  # current_key
                current_key = path[-1]

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

            # We update the path
            # The last key of path should always be right before a `buckets` key
            path.append('buckets')
            next_node = node
            next_node = next_node['buckets']
            if isinstance(buckets, list):
                path.append(0)
                next_node = next_node[0]
            elif isinstance(buckets, dict):
                path.append(first_key)
                next_node = next_node[first_key]

            # We find the next key if there is one
            next_key = [k for k in next_node.keys() if k not in ['key', 'doc_count']]
            if next_key:
                next_key = next_key[0]
                if 'buckets' in next_node[next_key]:
                    path.append(next_key)
                    current_key = next_key

            depth += 1

        return lines
