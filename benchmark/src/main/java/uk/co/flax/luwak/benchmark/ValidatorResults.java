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

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import uk.co.flax.luwak.DocumentMatches;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.QueryMatch;

public class ValidatorResults<T extends QueryMatch> extends BenchmarkResults<T> {

    private int correctMatches;
    private int total;
    private Multimap<String, T> missingMatches = HashMultimap.create();
    private Multimap<String, T> extraMatches = HashMultimap.create();

    public void add(Matches<T> matches, String docId, Set<T> expectedMatches) {
        super.add(matches);

        total++;
        DocumentMatches<T> docMatches = matches.getMatches(docId);
        if (docMatches == null) {
            missingMatches.putAll(docId, expectedMatches);
            return;
        }

        Set<T> actualMatches = Sets.newHashSet(docMatches);
        Sets.SetView<T> extras = Sets.difference(expectedMatches, actualMatches);
        Sets.SetView<T> missing = Sets.difference(actualMatches, expectedMatches);

        if (extras.isEmpty() && missing.isEmpty())
            correctMatches++;
        else {
            missingMatches.putAll(docMatches.getDocId(), missing);
            extraMatches.putAll(docMatches.getDocId(), extras);
        }

    }

    public int getCorrectMatchCount() {
        return correctMatches;
    }

    public int getTotalMatchCount() {
        return total;
    }

    public Collection<String> getBadDocuments() {
        return Sets.union(missingMatches.keySet(), extraMatches.keySet());
    }

    public Collection<T> getMissingMatches(String docId) {
        return missingMatches.get(docId);
    }

    public Collection<T> getExtraMatches(String docId) {
        return extraMatches.get(docId);
    }
}
