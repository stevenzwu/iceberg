# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


class PackingIterator:
    def __init__(self, items, target_weight, lookback, weight_func, largest_bin_first=False):
        self.items = iter(items)
        self.target_weight = target_weight
        self.lookback = lookback
        self.weight_func = weight_func
        self.largest_bin_first = largest_bin_first
        self.bins = list()

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            try:
                item = next(self.items)
                weight = self.weight_func(item)
                bin_ = self.find_bin(weight)
                if bin_ is not None:
                    bin_.add(item, weight)
                else:
                    bin_ = self.Bin(self.target_weight)
                    bin_.add(item, weight)
                    self.bins.append(bin_)

                    if len(self.bins) > self.lookback:
                        return list(self.remove_bin().items)
            except StopIteration:
                break

        if len(self.bins) == 0:
            raise StopIteration()

        return list(self.remove_bin().items)

    def find_bin(self, weight):
        for bin_ in self.bins:
            if bin_.can_add(weight):
                return bin_
        return None

    def remove_bin(self):
        if self.largest_bin_first:
            bin_ = max(self.bins, key=lambda b: b.weight())
            self.bins.remove(bin_)
            return bin_
        else:
            return self.bins.pop(0)

    class Bin:
        def __init__(self, target_weight: int):
            self.bin_weight = 0
            self.target_weight = target_weight
            self.items: list = list()

        def weight(self) -> int:
            return self.bin_weight

        def can_add(self, weight) -> bool:
            return self.bin_weight + weight <= self.target_weight

        def add(self, item, weight):
            self.bin_weight += weight
            self.items.append(item)
