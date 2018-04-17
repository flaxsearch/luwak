package uk.co.flax.luwak.presearcher;

import uk.co.flax.luwak.Presearcher;

public class TestTermFiltered extends FieldFilterPresearcherComponentTestBase {

    @Override
    protected Presearcher createPresearcher() {
        return new TermFilteredPresearcher(new FieldFilterPresearcherComponent("language"));
    }
}