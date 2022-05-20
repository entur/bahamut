package org.entur.bahamut.routes;

import java.util.Set;

public interface CSVHeader {
    String ID = "_id";
    String INDEX = "_index";
    String TYPE = "_type";
    String NAME = "name";
    String ALIAS = "name_json";
    String LATITUDE = "lat";
    String LONGITUDE = "lon";
    String ADDRESS_STREET = "street";
    String ADDRESS_NUMBER = "number";
    String ADDRESS_ZIP = "zipcode";
    String POPULARITY = "popularity";
    String CATEGORY = "category_json";
    String DESCRIPTION = "addendum_json_description";
    String SOURCE = "source";
}
