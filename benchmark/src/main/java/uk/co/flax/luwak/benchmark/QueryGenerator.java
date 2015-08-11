package uk.co.flax.luwak.benchmark;

/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class QueryGenerator {

    private final Random random;
    private final List<String> dictionary;

    public QueryGenerator(Random random, Set<String> dictionary) {
        this.random = random;
        this.dictionary = new ArrayList<>(dictionary.size());
        this.dictionary.addAll(dictionary);
    }

    public String newQuery() {
        if (random.nextBoolean()) {
            // simple term
            return newSimpleTermQuery(true);
        }
        return newBooleanQuery(random.nextInt(4));
    }

    protected String newSimpleTermQuery(boolean allowPhrase) {
        String term = dictionary.get(random.nextInt(dictionary.size()));
        if (random.nextInt(6) < 4) {
            return term;
        }
        if (allowPhrase && random.nextInt(6) < 4) {
            return "\"" + term + " " + newSimpleTermQuery(false) + "\"";
        }
        int suffix = random.nextInt(term.length());
        if (suffix > 3)
            return term.substring(0, suffix) + "*";
        return term;
    }

    protected String newBooleanQuery(int depth) {
        if (depth == 0)
            return newSimpleTermQuery(true);

        int size = random.nextInt(10);
        if (size > 8)
            size = random.nextInt(60);
        StringBuilder sb = new StringBuilder();

        sb.append("(").append(booleanOp()).append(booleanClause(depth));
        for (int i = 0; i < size; i++) {
            sb.append(" ").append(booleanOp()).append(booleanClause(depth));
        }
        return sb.append(")").toString();
    }

    protected String booleanOp() {
        switch (random.nextInt(6)) {
            case 0:
                return "-";
            case 1:
                return "+";
        }
        return "";
    }

    protected String booleanClause(int depth) {
        if (random.nextInt(4) > 2) {
            return newSimpleTermQuery(true);
        }
        else {
            return newBooleanQuery(Math.max(depth - 1 - random.nextInt(depth), 0));
        }
    }

    public static void main(String... args) {
        QueryGenerator gen = new QueryGenerator(new Random(), ImmutableSet.of("a", "b", "c", "d", "e", "fffffff", "gggggg"));
        for (int i = 0; i < 100; i++) {
            System.out.println(gen.newQuery());
        }
    }
}
