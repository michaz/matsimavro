/* *********************************************************************** *
 * project: org.matsim.*
 * NetworkWriter.java
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2007 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** */

package com.github.michaz.matsimavro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.log4j.Logger;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.api.internal.MatsimWriter;
import org.matsim.core.utils.geometry.CoordinateTransformation;
import org.matsim.core.utils.geometry.transformations.IdentityTransformation;
import org.matsim.core.utils.io.MatsimXmlWriter;
import org.matsim.core.utils.io.UncheckedIOException;
import org.matsim.utils.objectattributes.AttributeConverter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class AvroNetworkWriter extends MatsimXmlWriter implements MatsimWriter {

	private static final Logger log = Logger.getLogger(AvroNetworkWriter.class);

	private final Network network;
	private final CoordinateTransformation transformation;
	private final Map<Class<?>,AttributeConverter<?>> converters = new HashMap<>();
	private final Schema schema;

	public AvroNetworkWriter(final Network network) {
		this( new IdentityTransformation() , network );
	}

	public AvroNetworkWriter(
			final CoordinateTransformation transformation,
			final Network network) {
		this.transformation = transformation;
		this.network = network;
		try {
			this.schema = new Schema.Parser().parse(getClass().getResourceAsStream("network.avsc.json"));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public void putAttributeConverters(final Map<Class<?>, AttributeConverter<?>> converters) {
		this.converters.putAll( converters );
	}

	public void putAttributeConverter(Class<?> clazz , AttributeConverter<?> converter) {
		this.converters.put(  clazz , converter );
	}

	@Override
	public void write(final String filename) {
		File file = new File(filename);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
			dataFileWriter.create(schema, file);
			final GenericData.Record networkRecord = new GenericData.Record(schema);
			networkRecord.put("nodes", nodes(network));
			networkRecord.put("links", links(network));
			dataFileWriter.append(networkRecord);
			dataFileWriter.close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private Object nodes(Network network) {
		final Schema schema = this.schema.getField("nodes").schema();
		GenericData.Array<GenericRecord> nodes = new GenericData.Array<>(network.getNodes().size(), schema);
		network.getNodes().forEach((id, node) -> {
			final GenericData.Record nodeRecord = new GenericData.Record(schema.getElementType());
			nodeRecord.put("id", id.toString());
			nodeRecord.put("x", node.getCoord().getX());
			nodeRecord.put("y", node.getCoord().getY());
			nodes.add(nodeRecord);
		});
		return nodes;
	}


	private Object links(Network network) {
		final Schema schema = this.schema.getField("links").schema();
		GenericData.Array<GenericRecord> links = new GenericData.Array<>(network.getNodes().size(), schema);
		network.getLinks().forEach((id, link) -> {
			final GenericData.Record linkRecord = new GenericData.Record(schema.getElementType());
			linkRecord.put("id", id.toString());
			linkRecord.put("fromNode", link.getFromNode().getId().toString());
			linkRecord.put("toNode", link.getToNode().getId().toString());
			links.add(linkRecord);
		});
		return links;
	}


}
