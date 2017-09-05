package com.github.michaz.matsimavro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.population.Activity;
import org.matsim.api.core.v01.population.Leg;
import org.matsim.api.core.v01.population.Person;
import org.matsim.core.population.routes.NetworkRoute;
import org.matsim.core.utils.io.UncheckedIOException;
import org.matsim.vehicles.Vehicle;

import java.io.File;
import java.io.IOException;

public class AvroPersonWriter {

    private final Schema schema;
    private final Schema routeSchema;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private final Schema elementsSchema;
    private final Schema activitySchema;
    private final Schema legSchema;

    public AvroPersonWriter(File file) {
        try {
            schema = new Schema.Parser().parse(getClass().getResourceAsStream("person.avsc.json"));
            elementsSchema = this.schema.getField("plans").schema().getElementType().getField("elements").schema();
            activitySchema = elementsSchema.getElementType().getTypes().get(0);
            legSchema = elementsSchema.getElementType().getTypes().get(1);
            routeSchema = legSchema.getField("route").schema().getTypes().get(1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        try {
            dataFileWriter.create(schema, file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void append(Person person) {
        final GenericData.Record personRecord = new GenericData.Record(schema);
        personRecord.put("id", person.getId().toString());
        personRecord.put("plans", plans(person));
        try {
            dataFileWriter.append(personRecord);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Object plans(Person person) {
        final GenericData.Array plans = new GenericData.Array(person.getPlans().size(), schema.getField("plans").schema());
        person.getPlans().forEach(plan -> {
            final GenericData.Record avroPlan = new GenericData.Record(this.schema.getField("plans").schema().getElementType());
            avroPlan.put("score", plan.getScore());
            avroPlan.put("type", plan.getType());
            avroPlan.put("selected", plan == person.getSelectedPlan());
            final GenericData.Array elements = new GenericData.Array(person.getPlans().size(), elementsSchema);
            plan.getPlanElements().forEach(planElement -> {
                if (planElement instanceof Activity) {
                    Activity activity = (Activity) planElement;
                    final GenericData.Record avroActivity = new GenericData.Record(activitySchema);
                    avroActivity.put("x", activity.getCoord().getX());
                    avroActivity.put("y", activity.getCoord().getY());
                    if (activity.getCoord().hasZ()) {
                        avroActivity.put("z", activity.getCoord().getZ());
                    }
                    if (activity.getLinkId() != null) {
                        avroActivity.put("link", activity.getLinkId().toString());
                    }
                    if (activity.getFacilityId() != null) {
                        avroActivity.put("facility", activity.getFacilityId().toString());
                    }
                    avroActivity.put("start_time", activity.getStartTime());
                    avroActivity.put("end_time", activity.getEndTime());
                    avroActivity.put("max_dur", activity.getMaximumDuration());
                    elements.add(avroActivity);
                } else if (planElement instanceof Leg) {
                    Leg leg = (Leg) planElement;
                    final GenericData.Record avroLeg = new GenericData.Record(legSchema);
                    avroLeg.put("mode", leg.getMode());
                    avroLeg.put("route", route(leg));
                    avroLeg.put("dep_time", leg.getDepartureTime());
                    avroLeg.put("trav_time", leg.getTravelTime());
                    elements.add(avroLeg);
                }
            });
            avroPlan.put("elements", elements);
            plans.add(avroPlan);
        });
        return plans;
    }

    private Object route(Leg leg) {
        if (leg.getRoute() == null) {
            return null;
        } else {
            final GenericData.Record avroRoute = new GenericData.Record(routeSchema);
            avroRoute.put("description", leg.getRoute().getRouteDescription());
            avroRoute.put("start_link", leg.getRoute().getStartLinkId().toString());
            avroRoute.put("end_link", leg.getRoute().getStartLinkId().toString());
            avroRoute.put("trav_time", leg.getRoute().getTravelTime());
            avroRoute.put("distance", leg.getRoute().getDistance());
            if (leg.getRoute() instanceof NetworkRoute) {
                final Id<Vehicle> vehicleId = ((NetworkRoute) leg.getRoute()).getVehicleId();
                if (vehicleId != null) {
                    avroRoute.put("vehicleRefId", vehicleId);
                }
            }
            return avroRoute;
        }
    }

    public void close() {
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
