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

package org.entur.bahamut.camel.routes.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.geojson.Polygon;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PeliasDocument {

    public static final String DEFAULT_SOURCE = "whosonfirst";
    public static final Long DEFAULT_POPULARITY = 1L;

    // Valid sources for querying: "osm,oa,gn,wof,openstreetmap,openaddresses,geonames,whosonfirst"
    private String source = DEFAULT_SOURCE;
    private String layer;
    private String sourceId;
    private Map<String, String> nameMap;
    private Map<String, String> phraseMap;
    private Map<String, String> descriptionMap;
    private Map<String, String> aliasMap;
    private GeoPoint centerPoint;
    private Polygon shape;
    private String boundingBox;
    private AddressParts addressParts;
    private Parent parent;
    private long population;
    private long popularity = 1L;
    private List<String> category;
    private List<String> categoryFilter;
    private List<String> tariffZones;
    private List<String> tariffZoneAuthorities;

    public PeliasDocument(String layer, String sourceId) {
        this.layer = layer;
        this.sourceId = sourceId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getLayer() {
        return layer;
    }

    public void setLayer(String layer) {
        this.layer = layer;
    }

    public Map<String, String> getNameMap() {
        return nameMap;
    }

    public void setNameMap(Map<String, String> nameMap) {
        this.nameMap = nameMap;
    }

    public void setDefaultNameAndPhrase(String name) {
        addName("default", name);
        addPhrase("default", name);
    }

    @JsonIgnore
    public String getDefaultName() {
        if (nameMap != null) {
            return nameMap.get("default");
        }
        return null;
    }

    public void addName(String language, String name) {
        if (nameMap == null) {
            nameMap = new HashMap<>();
        }
        nameMap.put(language, name);
    }

    public void addDescription(String language, String description) {
        if (descriptionMap == null) {
            descriptionMap = new HashMap<>();
        }
        descriptionMap.put(language, description);
    }

    public Map<String, String> getDescriptionMap() {
        return descriptionMap;
    }

    public void setDescriptionMap(Map<String, String> descriptionMap) {
        this.descriptionMap = descriptionMap;
    }

    public void addAlias(String language, String alias) {
        if (aliasMap == null) {
            aliasMap = new HashMap<>();
        }
        aliasMap.put(language, alias);
    }

    public Map<String, String> getAliasMap() {
        return aliasMap;
    }

    public void setAliasMap(Map<String, String> aliasMap) {
        this.aliasMap = aliasMap;
    }

    @JsonIgnore
    public String getDefaultAlias() {
        if (aliasMap != null) {
            return aliasMap.get("default");
        }
        return null;
    }


    @JsonIgnore
    public String getDefaultPhrase() {
        if (descriptionMap != null) {
            return phraseMap.get("default");
        }
        return null;
    }

    public void addPhrase(String language, String phrase) {
        if (phraseMap == null) {
            phraseMap = new HashMap<>();
        }
        phraseMap.put(language, phrase);
    }

    public Map<String, String> getPhraseMap() {
        return phraseMap;
    }

    public void setPhraseMap(Map<String, String> phraseMap) {
        this.phraseMap = phraseMap;
    }

    public Polygon getShape() {
        return shape;
    }

    public void setShape(Polygon shape) {
        this.shape = shape;
    }

    public String getBoundingBox() {
        return boundingBox;
    }

    public void setBoundingBox(String boundingBox) {
        this.boundingBox = boundingBox;
    }

    public AddressParts getAddressParts() {
        return addressParts;
    }

    public void setAddressParts(AddressParts addressParts) {
        this.addressParts = addressParts;
    }

    public Parent getParent() {
        return parent;
    }

    public void setParent(Parent parent) {
        this.parent = parent;
    }

    public Long getPopulation() {
        return population;
    }

    public void setPopulation(Long population) {
        this.population = population;
    }

    public Long getPopularity() {
        return popularity;
    }

    public void setPopularity(Long popularity) {
        this.popularity = popularity;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public List<String> getCategory() {
        return category;
    }

    public void setCategory(List<String> category) {
        this.category = category;
        if (null != category) {
            this.categoryFilter = category.stream().map(String::toLowerCase).collect(Collectors.toList());
        }
    }

    public List<String> getCategoryFilter() {
        return categoryFilter;
    }

    public List<String> getTariffZones() {
        return tariffZones;
    }

    public void setTariffZones(List<String> tariffZones) {
        this.tariffZones = tariffZones;
    }

    public List<String> getTariffZoneAuthorities() {
        return tariffZoneAuthorities;
    }

    public void setTariffZoneAuthorities(List<String> tariffZoneAuthorities) {
        this.tariffZoneAuthorities = tariffZoneAuthorities;
    }

    public GeoPoint getCenterPoint() {
        return centerPoint;
    }

    public void setCenterPoint(GeoPoint centerPoint) {
        this.centerPoint = centerPoint;
    }

    @JsonIgnore
    public boolean isValid() {
        return source != null && sourceId != null && layer != null;
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