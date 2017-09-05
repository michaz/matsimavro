package com.github.michaz.matsimavro;

import org.junit.Test;
import org.matsim.api.core.v01.Scenario;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.io.IOUtils;
import org.matsim.examples.ExamplesUtils;

import java.io.File;

public class AvroPersonWriterTest {

    @Test
    public void avroWriterTest() {
        final Scenario equil = ScenarioUtils.loadScenario(ConfigUtils.loadConfig(IOUtils.newUrl(ExamplesUtils.getTestScenarioURL("equil"), "config.xml")));
        final AvroPersonWriter avroPersonWriter = new AvroPersonWriter(new File("wurst.avro"));
        equil.getPopulation().getPersons().forEach((id, person) -> {
            avroPersonWriter.append(person);
        });
        avroPersonWriter.close();
    }

}
