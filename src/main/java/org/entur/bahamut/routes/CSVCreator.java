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
            } else if (json) {
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

    public InputStream create(List<ElasticsearchCommand> commands) {

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
                map.put(SOURCE_ID, CSVValue(peliasDocument.getSourceId()));
                map.put(LAYER, CSVValue(peliasDocument.getLayer()));
            }
            return map;
        }).toList();

        List<String[]> stringArrays = csvCommands.stream().map(csvCommand -> headers.stream()
                        .map(header -> csvCommand.computeIfAbsent(header, h -> CSVValue("")))
                        .map(CSVValue::toString)
                        .toArray(String[]::new))
                .toList();

//        File file = new File("/Users/mansoor.sajjad/local-gcs-storage/bahamut/output.csv");
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


/*
        PipedInputStream in = new PipedInputStream();
        try (PipedOutputStream out = new PipedOutputStream(in)) {
            new Thread(
                    () -> {
                        try {
                            outputStream.writeTo(out);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
            ).start();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to copy with pipe");
        }

        try {
            OutputStream outStream = new FileOutputStream(file);

            byte[] buffer = new byte[8 * 1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(outStream);

        } catch (Exception ex) {

        }

 */

        /*
        try (OutputStream outStream = new FileOutputStream(file)) {
            InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());

            byte[] buffer = new byte[8 * 1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }

        } catch (Exception ex) {
            throw new RuntimeException("Failed: :(");
        }

         */

    }
}
