package com.github.michaz.matsimavro;

import org.junit.Test;
import org.matsim.api.core.v01.Scenario;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.io.IOUtils;
import org.matsim.examples.ExamplesUtils;

public class AvroNetworkWriterTest {

    @Test
    public void avroWriterTest() {
        final Scenario equil = ScenarioUtils.loadScenario(ConfigUtils.loadConfig(IOUtils.newUrl(ExamplesUtils.getTestScenarioURL("equil"), "config.xml")));
        new AvroNetworkWriter(equil.getNetwork()).write("wurst.avro");
    }

}
