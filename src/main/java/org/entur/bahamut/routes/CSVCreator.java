package org.entur.bahamut.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import org.entur.bahamut.routes.json.PeliasDocument;

import java.io.*;
import java.util.*;

import static org.entur.bahamut.routes.CSVHeader.*;

public class CSVCreator {

    private record CSVValue(Object value, boolean json) {
        @Override
        public String toString() {
            if (value == null) {
                return "";
            }
            if (json) {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    StringWriter writer = new StringWriter();
                    mapper.writeValue(writer, value);
                    return writer.toString();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return value.toString();
            }
        }
    }

    private static CSVValue CSVValue(Object value) {
        return new CSVValue(value, false);
    }

    private static CSVValue CSVJsonValue(Object value) {
        return new CSVValue(value, true);
    }

    public void create(List<ElasticsearchCommand> commands) {

        Set<String> headers = new java.util.HashSet<>();
        headers.add(CSVHeader.ID);
        headers.add(CSVHeader.INDEX);
        headers.add(CSVHeader.TYPE);
        headers.add(CSVHeader.NAME);
        headers.add(CSVHeader.ALIAS);
        headers.add(CSVHeader.LATITUDE);
        headers.add(CSVHeader.LONGITUDE);
        headers.add(CSVHeader.ADDRESS_STREET);
        headers.add(CSVHeader.ADDRESS_NUMBER);
        headers.add(CSVHeader.ADDRESS_ZIP);
        headers.add(CSVHeader.POPULARITY);
        headers.add(CSVHeader.CATEGORY);
        headers.add(CSVHeader.DESCRIPTION);

        List<Map<String, CSVValue>> csvCommands = commands.stream().map(command -> {
            Map<String, CSVValue> map = new HashMap<>();
            map.put(ID, CSVValue(command.getIndex().getId()));
            map.put(INDEX, CSVValue(command.getIndex().getIndex()));
            map.put(TYPE, CSVValue(command.getIndex().getType()));

            if (command.getSource() instanceof PeliasDocument peliasDocument) {
                map.put(NAME, CSVValue(peliasDocument.getDefaultName()));
                peliasDocument.getNameMap().entrySet().stream()
                        .filter(entry -> !entry.getKey().equals("default"))
                        .forEach(entry -> {
                            String header = NAME + "_" + entry.getKey();
                            headers.add(header);
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
                                headers.add(header);
                                map.put(header, CSVJsonValue(List.of(entry.getValue())));
                            });
                }
                if (peliasDocument.getCenterPoint() != null) {
                    map.put(LATITUDE, CSVValue(peliasDocument.getCenterPoint().getLat()));
                    map.put(LONGITUDE, CSVValue(peliasDocument.getCenterPoint().getLon()));
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
            }
            return map;
        }).toList();

        List<String[]> stringArrays = csvCommands.stream().map(csvCommand -> headers.stream()
                        .map(header -> csvCommand.computeIfAbsent(header, h -> CSVValue("")))
                        .map(CSVValue::toString)
                        .toArray(String[]::new))
                .toList();

        File file = new File("/Users/mansoor.sajjad/local-gcs-storage/bahamut/output.csv");
        try (CSVWriter writer = new CSVWriter(new FileWriter(file.getAbsolutePath()))) {
            writer.writeNext(headers.toArray(String[]::new));
            for (String[] array : stringArrays) {
                writer.writeNext(array);
            }
        } catch (IOException exception) {
            throw new RuntimeException("Fail to create csv.", exception);
        }
    }
}
