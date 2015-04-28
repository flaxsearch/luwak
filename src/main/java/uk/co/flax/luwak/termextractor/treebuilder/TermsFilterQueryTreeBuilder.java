package uk.co.flax.luwak.termextractor.treebuilder;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.util.BytesRef;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.DisjunctionNode;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TermNode;

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

public class TermsFilterQueryTreeBuilder extends QueryTreeBuilder<TermsFilter> {

    private static Field termsField;
    private static Field termsBytesField;
    private static Field startField;
    private static Field endField;
    private static Field fieldNameField;
    private static Field offsetsField;

    static {
        Field[] allFields = TermsFilter.class.getDeclaredFields();
        for (Field field : allFields) {
            if ("termsAndFields".equals(field.getName())) {
                field.setAccessible(true);
                termsField = field;
            } else if ("termsBytes".equals(field.getName())) {
                field.setAccessible(true);
                termsBytesField = field;
            } else if ("offsets".equals(field.getName())) {
                field.setAccessible(true);
                offsetsField = field;
            }
        }
        Class<?>[] innerClasses = TermsFilter.class.getDeclaredClasses();
        for (Class<?> innerClass : innerClasses) {
            if ("org.apache.lucene.queries.TermsFilter$TermsAndField".equals(innerClass.getName())) {
                try {
                    startField = innerClass.getDeclaredField("start");
                    startField.setAccessible(true);
                    endField = innerClass.getDeclaredField("end");
                    endField.setAccessible(true);
                    fieldNameField = innerClass.getDeclaredField("field");
                    fieldNameField.setAccessible(true);
                } catch (NoSuchFieldException | SecurityException ex) {
                    throw new RuntimeException("Couldn't initialize TermsFilterTermExtractor", ex);
                }
            }
        }
    }

    public TermsFilterQueryTreeBuilder() {
        super(TermsFilter.class);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, TermsFilter query) {
        try {
            List<QueryTree> children = new ArrayList<>();
            for (Term term : getTermsFromTermsFilter(query)) {
                children.add(new TermNode(new QueryTerm(term)));
            }
            return DisjunctionNode.build(children);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Couldn't extract terms from TermsFilter", e);
        }
    }

    private List<Term> getTermsFromTermsFilter(TermsFilter termsFilter) throws IllegalAccessException {
        List<Term> result = new LinkedList<>();
        termsField.get(termsFilter);
        byte[] termsBytes = (byte[]) termsBytesField.get(termsFilter);
        int[] offsets = (int[]) offsetsField.get(termsFilter);
        final BytesRef spare = new BytesRef(termsBytes);

        Object[] termsAndFields = (Object[]) termsField.get(termsFilter);
        for (Object term : termsAndFields) {
            int start = (int) startField.get(term);
            int end = (int) endField.get(term);
            String field = (String) fieldNameField.get(term);
            for (int i = start; i < end; i++) {
                spare.offset = offsets[i];
                spare.length = offsets[i + 1] - offsets[i];
                result.add(new Term(field, spare.clone()));
            }
        }
        return result;
    }
}
