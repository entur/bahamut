package org.entur.bahamut.camel.csv;

import com.opencsv.CSVWriter;
import org.entur.bahamut.camel.routes.json.PeliasDocument;

import java.io.*;
import java.util.*;

import static org.entur.bahamut.camel.csv.CSVHeaders.*;

public final class CSVCreator {

    private static CSVValue CSVValue(Object value) {
        return new CSVValue(value, false);
    }

    private static CSVValue CSVJsonValue(Object value) {
        return new CSVValue(value, true);
    }

    public InputStream create(List<PeliasDocument> peliasDocuments) {

        Set<String> headers = new java.util.HashSet<>();
        headers.add(ID);
        headers.add(INDEX);
        headers.add(TYPE);
        headers.add(NAME);
        headers.add(ALIAS);
        headers.add(LATITUDE);
        headers.add(LONGITUDE);
        headers.add(ADDRESS_STREET);
        headers.add(ADDRESS_NUMBER);
        headers.add(ADDRESS_ZIP);
        headers.add(POPULARITY);
        headers.add(CATEGORY);
        headers.add(DESCRIPTION);
        headers.add(SOURCE);
        headers.add(SOURCE_ID);
        headers.add(LAYER);
        headers.add(PARENT);

        List<Map<String, CSVValue>> csvDocuments = peliasDocuments.stream().map(document -> {
            Map<String, CSVValue> map = new HashMap<>();
            map.put(ID, CSVValue(document.getSourceId()));
            map.put(INDEX, CSVValue(document.getIndex()));
            map.put(TYPE, CSVValue(document.getLayer()));

            map.put(NAME, CSVValue(document.getDefaultName()));
            document.getNameMap().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals("default"))
                    .forEach(entry -> {
                        String header = NAME + "_" + entry.getKey();
                        headers.add(header);
                        map.put(header, CSVValue(entry.getValue()));
                    });
            if (document.getDefaultAlias() != null) {
                map.put(ALIAS, CSVJsonValue(List.of(document.getDefaultAlias())));
            }
            if (document.getAliasMap() != null) {
                document.getAliasMap().entrySet().stream()
                        .filter(entry -> !entry.getKey().equals("default"))
                        .forEach(entry -> {
                            String header = ALIAS + "_" + entry.getKey();
                            headers.add(header);
                            map.put(header, CSVJsonValue(List.of(entry.getValue())));
                        });
            }
            if (document.getCenterPoint() != null) {
                map.put(LATITUDE, CSVValue(document.getCenterPoint().lat()));
                map.put(LONGITUDE, CSVValue(document.getCenterPoint().lon()));
            }
            if (document.getAddressParts() != null) {
                map.put(ADDRESS_STREET, CSVValue(document.getAddressParts().getStreet()));
                map.put(ADDRESS_NUMBER, CSVValue(document.getAddressParts().getNumber()));
                map.put(ADDRESS_ZIP, CSVValue(document.getAddressParts().getZip()));
            }
            map.put(POPULARITY, CSVValue(document.getPopularity()));
            map.put(CATEGORY, CSVJsonValue(document.getCategory()));
            map.put(DESCRIPTION, CSVJsonValue(document.getDescriptionMap()));
            map.put(SOURCE, CSVValue(document.getSource()));
            map.put(SOURCE_ID, CSVValue(document.getSourceId()));
            map.put(LAYER, CSVValue(document.getLayer()));
            map.put(PARENT, CSVJsonValue(document.getParent().getParentFields()));
            return map;
        }).toList();

        List<String[]> stringArrays = csvDocuments.stream().map(csvDocument -> headers.stream()
                        .map(header -> csvDocument.computeIfAbsent(header, h -> CSVValue("")))
                        .map(CSVValue::toString)
                        .toArray(String[]::new))
                .toList();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(outputStream))) {
            writer.writeNext(headers.toArray(String[]::new));
            for (String[] array : stringArrays) {
                writer.writeNext(array);
            }
        } catch (IOException exception) {
            throw new RuntimeException("Fail to create csv.", exception);
        }

        return new ByteArrayInputStream(outputStream.toByteArray());
    }
}
