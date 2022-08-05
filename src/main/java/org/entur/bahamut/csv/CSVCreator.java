package org.entur.bahamut.csv;

import com.opencsv.CSVWriter;
import org.entur.bahamut.peliasDocument.model.Parent;
import org.entur.bahamut.peliasDocument.model.PeliasDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.entur.bahamut.csv.CSVHeaders.*;

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
        map.put(ID, CSVValue(peliasDocument.sourceId()));
        map.put(INDEX, CSVValue(peliasDocument.index()));
        map.put(TYPE, CSVValue(peliasDocument.layer()));

        map.put(NAME, CSVValue(peliasDocument.defaultName()));
        peliasDocument.getNameMap().entrySet().stream()
                .filter(entry -> !entry.getKey().equals("default"))
                .forEach(entry -> {
                    String header = NAME + "_" + entry.getKey();
                    addNewHeader.accept(header);
                    map.put(header, CSVValue(entry.getValue()));
                });
        if (peliasDocument.defaultAlias() != null) {
            map.put(ALIAS, CSVJsonValue(List.of(peliasDocument.defaultAlias())));
        }
        if (peliasDocument.aliasMap() != null) {
            peliasDocument.aliasMap().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals("default"))
                    .forEach(entry -> {
                        String header = ALIAS + "_" + entry.getKey();
                        addNewHeader.accept(header);
                        map.put(header, CSVJsonValue(List.of(entry.getValue())));
                    });
        }
        if (peliasDocument.centerPoint() != null) {
            map.put(LATITUDE, CSVValue(peliasDocument.centerPoint().lat()));
            map.put(LONGITUDE, CSVValue(peliasDocument.centerPoint().lon()));
        }
        if (peliasDocument.addressParts() != null) {
            map.put(ADDRESS_STREET, CSVValue(peliasDocument.addressParts().getStreet()));
            map.put(ADDRESS_NUMBER, CSVValue(peliasDocument.addressParts().getNumber()));
            map.put(ADDRESS_ZIP, CSVValue(peliasDocument.addressParts().getZip()));
        }
        map.put(POPULARITY, CSVValue(peliasDocument.popularity()));
        map.put(CATEGORY, CSVJsonValue(peliasDocument.category()));
        map.put(DESCRIPTION, CSVJsonValue(peliasDocument.descriptionMap()));
        map.put(SOURCE, CSVValue(peliasDocument.source()));
        map.put(SOURCE_ID, CSVValue(peliasDocument.sourceId()));
        map.put(LAYER, CSVValue(peliasDocument.layer()));
        if (peliasDocument.parent() != null) {
            map.put(PARENT, CSVJsonValue(wrapParentFieldsInLists(peliasDocument.parent().getParentFields())));
        }

        return map;
    }


    /**
     * See the following comment to learn why we need to do this.
     * https://github.com/pelias/csv-importer/pull/97#issuecomment-1203920795
     */
    private static Map<Parent.FieldName, List<Parent.Field>> wrapParentFieldsInLists(Map<Parent.FieldName, Parent.Field> parentFields) {
        return parentFields.entrySet()
                .stream()
                // TODO: Some of the localities have no name. Which is not acceptable by the pelias model. Id and name are mandatory field.
                //  We are filtering them out for now, but we need to find, why they are missing names and how we can fix them.
                //  Does localities have even names in Netex file we are getting from tiamat.
                .filter(entry -> entry.getValue().isValid())
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> List.of(entry.getValue())));
    }

    private static CSVValue CSVValue(Object value) {
        return new CSVValue(value, false);
    }

    private static CSVValue CSVJsonValue(Object value) {
        return new CSVValue(value, true);
    }
}
