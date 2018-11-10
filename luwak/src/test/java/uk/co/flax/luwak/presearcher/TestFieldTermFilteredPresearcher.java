package uk.co.flax.luwak.presearcher;

import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

public class TestFieldTermFilteredPresearcher extends FieldFilterPresearcherComponentTestBase {

    @Override
    protected Presearcher createPresearcher() {
        return new TermFilteredPresearcher(new FieldFilterPresearcherComponent("language"));
    }
}
