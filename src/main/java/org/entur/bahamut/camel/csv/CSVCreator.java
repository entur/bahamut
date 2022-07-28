package org.entur.bahamut.camel.csv;

import com.opencsv.CSVWriter;
import org.entur.bahamut.camel.routes.json.PeliasDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.entur.bahamut.camel.csv.CSVHeaders.*;

public final class CSVCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVCreator.class);

    public InputStream create(List<PeliasDocument> peliasDocuments) {

        LOGGER.debug("Creating CSV file for " + peliasDocuments.size() + " pelias documents");

        var headers = Stream
                .of(ID, INDEX, TYPE, NAME, ALIAS, LATITUDE, LONGITUDE, ADDRESS_STREET, ADDRESS_NUMBER, ADDRESS_ZIP,
                        POPULARITY, CATEGORY, DESCRIPTION, SOURCE, SOURCE_ID, LAYER, PARENT)
                .collect(Collectors.toCollection(HashSet::new));

        var csvDocuments = peliasDocuments.stream()
                .map(document -> createCSVDocument(document, headers::add))
                .toList();

        var stringArrays = csvDocuments.stream()
                .map(csvDocument -> headers.stream()
                        .map(header -> csvDocument.computeIfAbsent(header, h -> CSVValue("")))
                        .map(CSVValue::toString)
                        .toArray(String[]::new))
                .toList();

        ByteArrayOutputStream outputStream = writeStringArraysToCSVFile(stringArrays, headers);

        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private ByteArrayOutputStream writeStringArraysToCSVFile(List<String[]> stringArrays,
                                                             Set<String> headers) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (var writer = new CSVWriter(new OutputStreamWriter(outputStream))) {
            writer.writeNext(headers.toArray(String[]::new));
            for (String[] array : stringArrays) {
                writer.writeNext(array);
            }
        } catch (IOException exception) {
            throw new RuntimeException("Fail to create csv.", exception);
        }

        return outputStream;
    }

    private static HashMap<String, CSVValue> createCSVDocument(PeliasDocument peliasDocument, Consumer<String> addNewHeader) {
        var map = new HashMap<String, CSVValue>();
        map.put(ID, CSVValue(peliasDocument.getSourceId()));
        map.put(INDEX, CSVValue(peliasDocument.getIndex()));
        map.put(TYPE, CSVValue(peliasDocument.getLayer()));

        map.put(NAME, CSVValue(peliasDocument.getDefaultName()));
        peliasDocument.getNameMap().entrySet().stream()
                .filter(entry -> !entry.getKey().equals("default"))
                .forEach(entry -> {
                    String header = NAME + "_" + entry.getKey();
                    addNewHeader.accept(header);
                    map.put(header, CSVValue(entry.getValue()));
                });
        if (peliasDocument.getDefaultAlias() != null) {
            map.put(ALIAS, CSVJsonValue(List.of(peliasDocument.getDefaultAlias())));
        }
        if (peliasDocument.getAliasMap() != null) {
            peliasDocument.getAliasMap().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals("default"))
                    .forEach(entry -> {
                        String header = ALIAS + "_" + entry.getKey();
                        addNewHeader.accept(header);
                        map.put(header, CSVJsonValue(List.of(entry.getValue())));
                    });
        }
        if (peliasDocument.getCenterPoint() != null) {
            map.put(LATITUDE, CSVValue(peliasDocument.getCenterPoint().lat()));
            map.put(LONGITUDE, CSVValue(peliasDocument.getCenterPoint().lon()));
        }
        if (peliasDocument.getAddressParts() != null) {
            map.put(ADDRESS_STREET, CSVValue(peliasDocument.getAddressParts().getStreet()));
            map.put(ADDRESS_NUMBER, CSVValue(peliasDocument.getAddressParts().getNumber()));
            map.put(ADDRESS_ZIP, CSVValue(peliasDocument.getAddressParts().getZip()));
        }
        map.put(POPULARITY, CSVValue(peliasDocument.getPopularity()));
        map.put(CATEGORY, CSVJsonValue(peliasDocument.getCategory()));
        map.put(DESCRIPTION, CSVJsonValue(peliasDocument.getDescriptionMap()));
        map.put(SOURCE, CSVValue(peliasDocument.getSource()));
        map.put(SOURCE_ID, CSVValue(peliasDocument.getSourceId()));
        map.put(LAYER, CSVValue(peliasDocument.getLayer()));
        if (peliasDocument.getParent() != null) {
            map.put(PARENT, CSVJsonValue(peliasDocument.getParent().getParentFields()));
        }

        return map;
    }

    private static CSVValue CSVValue(Object value) {
        return new CSVValue(value, false);
    }

    private static CSVValue CSVJsonValue(Object value) {
        return new CSVValue(value, true);
    }
}
