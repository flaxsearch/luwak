package uk.co.flax.luwak.termextractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

public class TermsFilterTermExtractor implements FilterTermExtractor {

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

    @Override
    public void extract(Filter filter, List<QueryTerm> terms) {
        if (filter.getClass() == TermsFilter.class) {
            try {
                List<Term> filterTerms = getTermsFromTermsFilter((TermsFilter) filter);
                for (Term term : filterTerms) {
                    terms.add(new QueryTerm(term.field(), term.text(), QueryTerm.Type.EXACT));
                }
            } catch (IllegalAccessException ex) {
                throw new RuntimeException("Couldn't extract terms from filter", ex);
            }
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
