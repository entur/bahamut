/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 *
 */

package org.entur.bahamut.peliasDocument.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.geojson.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

public class PeliasDocument {

    private static final Logger logger = LoggerFactory.getLogger(PeliasDocument.class);

    private final String layer;
    private final String sourceId;

    private GeoPoint centerPoint;
    private Polygon shape;
    private String boundingBox;
    private AddressParts addressParts;
    private Parent parent;
    private long population;
    private long popularity = 1L;

    private final Map<String, String> nameMap = new HashMap<>();
    private final Map<String, String> phraseMap = new HashMap<>();
    private final Map<String, String> descriptionMap = new HashMap<>();
    private final Map<String, String> aliasMap = new HashMap<>();

    private List<String> category = new ArrayList<>();
    private List<String> categoryFilter = new ArrayList<>();
    private List<String> tariffZones = new ArrayList<>();
    private List<String> tariffZoneAuthorities = new ArrayList<>();

    public PeliasDocument(String layer, String sourceId) {
        this.layer = Objects.requireNonNull(layer);
        this.sourceId = Objects.requireNonNull(sourceId);
    }

    public String index() {
        return "pelias";
    }

    public String source() {
        return "nsr";
    }

    public String layer() {
        return layer;
    }

    public Map<String, String> getNameMap() {
        return nameMap;
    }

    public void setDefaultNameAndPhrase(String name) {
        addName("default", name);
        addPhrase("default", name);
    }

    public String defaultName() {
        return nameMap.get("default");
    }

    public void addName(String language, String name) {
        nameMap.put(language, name);
    }

    public void addDescription(String language, String description) {
        descriptionMap.put(language, description);
    }

    public Map<String, String> descriptionMap() {
        return descriptionMap;
    }

    public void addAlias(String language, String alias) {
        aliasMap.put(language, alias);
    }

    public Map<String, String> aliasMap() {
        return aliasMap;
    }

    public String defaultAlias() {
        return aliasMap.get("default");
    }

    public String defaultPhrase() {
        return phraseMap.get("default");
    }

    public void addPhrase(String language, String phrase) {
        phraseMap.put(language, phrase);
    }

    public Map<String, String> phraseMap() {
        return phraseMap;
    }

    public Polygon shape() {
        return shape;
    }

    public void setShape(Polygon shape) {
        this.shape = shape;
    }

    public String boundingBox() {
        return boundingBox;
    }

    public void setBoundingBox(String boundingBox) {
        this.boundingBox = boundingBox;
    }

    public AddressParts addressParts() {
        return addressParts;
    }

    public void setAddressParts(AddressParts addressParts) {
        this.addressParts = addressParts;
    }

    public Parent parent() {
        return parent;
    }

    public void setParent(Parent parent) {
        this.parent = parent;
    }

    public Long population() {
        return population;
    }

    public void setPopulation(Long population) {
        this.population = population;
    }

    public Long popularity() {
        return popularity;
    }

    public void setPopularity(Long popularity) {
        this.popularity = popularity;
    }

    public String sourceId() {
        return sourceId;
    }

    public List<String> category() {
        return category;
    }

    public void setCategory(List<String> category) {
        this.category = category;
        if (null != category) {
            this.categoryFilter = category.stream().map(String::toLowerCase).collect(Collectors.toList());
        }
    }

    public List<String> categoryFilter() {
        return categoryFilter;
    }

    public List<String> tariffZones() {
        return tariffZones;
    }

    public void setTariffZones(List<String> tariffZones) {
        this.tariffZones = tariffZones;
    }

    public List<String> tariffZoneAuthorities() {
        return tariffZoneAuthorities;
    }

    public void setTariffZoneAuthorities(List<String> tariffZoneAuthorities) {
        this.tariffZoneAuthorities = tariffZoneAuthorities;
    }

    public GeoPoint centerPoint() {
        return centerPoint;
    }

    public void setCenterPoint(GeoPoint centerPoint) {
        this.centerPoint = centerPoint;
    }

    @JsonIgnore
    public boolean isValid() {

        if (centerPoint == null) {
            logger.debug("Removing invalid document where geometry is missing:" + this);
            return false;
        }
        return true;
    }

    public String toString() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            StringWriter writer = new StringWriter();
            mapper.writeValue(writer, this);
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}