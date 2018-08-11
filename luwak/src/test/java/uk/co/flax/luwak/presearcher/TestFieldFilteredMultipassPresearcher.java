package uk.co.flax.luwak.presearcher;

import uk.co.flax.luwak.Presearcher;

public class TestFieldFilteredMultipassPresearcher extends FieldFilterPresearcherComponentTestBase {

    @Override
    protected Presearcher createPresearcher() {
        return new MultipassTermFilteredPresearcher(2, 0.0f, new FieldFilterPresearcherComponent("language"));
    }
}
