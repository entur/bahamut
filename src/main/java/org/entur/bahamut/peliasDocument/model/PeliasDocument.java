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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PeliasDocument {

    private static final Logger logger = LoggerFactory.getLogger(PeliasDocument.class);
    public static final String DEFAULT_INDEX = "pelias";
    public static final String DEFAULT_SOURCE = "nsr";

    private final String layer;
    private final String sourceId;

    private GeoPoint centerPoint;
    private AddressParts addressParts;
    private Parent parent;
    private long popularity = 1L;

    private final Map<String, String> nameMap = new HashMap<>();
    private final Map<String, String> descriptionMap = new HashMap<>();
    private final Map<String, String> aliasMap = new HashMap<>();

    private List<String> category = new ArrayList<>();
    private List<String> tariffZones = new ArrayList<>();
    private List<String> tariffZoneAuthorities = new ArrayList<>();

    public PeliasDocument(String layer, String sourceId) {
        this.layer = Objects.requireNonNull(layer);
        this.sourceId = Objects.requireNonNull(sourceId);
    }

    public String layer() {
        return layer;
    }

    public Set<Map.Entry<String, String>> namesEntrySet() {
        return nameMap.entrySet();
    }

    public String defaultName() {
        return nameMap.get("default");
    }

    public void addName(String language, String name) {
        nameMap.put(IsoLanguageCodeMap.getLanguage(language), name);
    }

    public void addDefaultName(String name) {
        nameMap.put("default", name);
    }

    public void addDisplayName(String name) {
        nameMap.put("display", name);
    }

    public void addDescription(String language, String description) {
        descriptionMap.put(language, description);
    }

    public Map<String, String> descriptionMap() {
        return descriptionMap;
    }

    public void addAlias(String language, String alias) {
        aliasMap.put(IsoLanguageCodeMap.getLanguage(language), alias);
    }

    public void addDefaultAlias(String alias) {
        aliasMap.put("default", alias);
    }

    public Map<String, String> aliasMap() {
        return aliasMap;
    }

    public String defaultAlias() {
        return aliasMap.get("default");
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
}