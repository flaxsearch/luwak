package uk.co.flax.luwak.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * Copyright (c) 2014 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public final class CollectionUtils {

    @SafeVarargs
    public static <T> List<T> makeUnmodifiableList(T... items) {
        ArrayList<T> list = new ArrayList<>(items.length);
        Collections.addAll(list, items);
        return Collections.unmodifiableList(list);
    }

    public static <T> List<List<T>> partition(List<T> items, int slices) {
        double size = items.size() / (double) slices;
        double accum = 0;
        int start = 0;
        List<List<T>> list = new ArrayList<>(slices);
        for (int i = 0; i < slices; i++) {
            int end = (int) Math.floor(accum + size);
            if (i == slices - 1)
                end = items.size();
            list.add(items.subList(start, end));
            accum += size;
            start = (int) Math.floor(accum);
        }
        return list;
    }
}
