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

package org.entur.bahamut.peliasDocument.toPeliasDocument;


import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.entur.bahamut.peliasDocument.model.GeoPoint;
import org.entur.bahamut.peliasDocument.model.PeliasDocument;
import org.entur.bahamut.peliasDocument.stopPlaceHierarchy.StopPlaceHierarchy;
import org.entur.bahamut.peliasDocument.model.Parent;
import org.rutebanken.netex.model.*;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.entur.bahamut.peliasDocument.toPeliasDocument.ToPeliasDocumentUtilities.DEFAULT_LANGUAGE;

public class StopPlaceHierarchiesToPeliasDocument {

    public static final String STOP_PLACE_LAYER = "stop_place";
    public static final String PARENT_STOP_PLACE_LAYER = "stop_place_parent";
    public static final String CHILD_STOP_PLACE_LAYER = "stop_place_child";
    private static final String KEY_IS_PARENT_STOP_PLACE = "IS_PARENT_STOP_PLACE";
    private final StopPlaceBoostConfiguration boostConfiguration;

    public StopPlaceHierarchiesToPeliasDocument(StopPlaceBoostConfiguration boostConfiguration) {
        super();
        this.boostConfiguration = boostConfiguration;
    }

    /**
     * Map single place hierarchy to (potentially) multiple pelias documents, one per alias/alternative name.
     * * <p>
     * Pelias does not yet support queries in multiple languages / for aliases.
     * When support for this is ready this mapping should be refactored to produce
     * a single document per place hierarchy.
     */
    public List<PeliasDocument> toPeliasDocuments(StopPlaceHierarchy placeHierarchy) {
        StopPlace place = placeHierarchy.getPlace();
        if (!isValid(place)) {
            return new ArrayList<>();
        }
        var cnt = new AtomicInteger();

        return getNames(placeHierarchy).stream()
                .map(name -> toPeliasDocument(placeHierarchy, name, cnt.getAndAdd(1)))
                .collect(Collectors.toList());
    }

    private PeliasDocument toPeliasDocument(StopPlaceHierarchy placeHierarchy, MultilingualString name, int idx) {
        StopPlace place = placeHierarchy.getPlace();

        var idSuffix = idx > 0 ? "-" + idx : "";

        var document = new PeliasDocument(getLayer(placeHierarchy), place.getId() + idSuffix);
        if (name != null) {
            document.setDefaultNameAndPhrase(name.getValue());
        }

        // Add official name as display name. Not a part of standard pelias model,
        // will be copied to name.default before deduping and labelling in Entur-pelias API.
        var displayName = getDisplayName(placeHierarchy);
        if (displayName != null) {
            document.getNameMap().put("display", displayName.getValue());
            if (displayName.getLang() != null) {
                document.addName(displayName.getLang(), displayName.getValue());
            }
        }

        // StopPlaces
        if (place.getCentroid() != null) {
            var loc = place.getCentroid().getLocation();
            document.setCenterPoint(new GeoPoint(loc.getLatitude().doubleValue(), loc.getLongitude().doubleValue()));
        }

        ToPeliasDocumentUtilities.addIdToStreetNameToAvoidFalseDuplicates(place.getId(), document);

        if (place.getDescription() != null && !StringUtils.isEmpty(place.getDescription().getValue())) {
            var lang = place.getDescription().getLang();
            if (lang == null) {
                lang = DEFAULT_LANGUAGE;
            }
            document.addDescription(lang, place.getDescription().getValue());
        }

        populateDocument(placeHierarchy, document);
        return document;
    }

    /**
     * Get name from current place or, if not set, on closest parent with name set.
     */
    private MultilingualString getDisplayName(StopPlaceHierarchy placeHierarchy) {
        if (placeHierarchy.getPlace().getName() != null) {
            return placeHierarchy.getPlace().getName();
        }
        if (placeHierarchy.getParent() != null) {
            return getDisplayName(placeHierarchy.getParent());
        }
        return null;
    }

    private boolean isValid(StopPlace place) {
        // Ignore rail replacement bus
        if (VehicleModeEnumeration.BUS.equals(place.getTransportMode())
                && BusSubmodeEnumeration.RAIL_REPLACEMENT_BUS.equals(place.getBusSubmode())) {
            return false;
        }

        // Skip stops without quays, unless they are parent stops
        if (isQuayLessNonParentStop(place)) {
            return false;
        }

        return ToPeliasDocumentUtilities.isValid(place);
    }

    private List<MultilingualString> getNames(StopPlaceHierarchy placeHierarchy) {
        List<MultilingualString> names = new ArrayList<>();

        collectNames(placeHierarchy, names, true);
        collectNames(placeHierarchy, names, false);

        return ToPeliasDocumentUtilities.filterUnique(names);
    }

    private void collectNames(StopPlaceHierarchy placeHierarchy, List<MultilingualString> names, boolean up) {
        StopPlace place = placeHierarchy.getPlace();
        if (place.getName() != null) {
            names.add(placeHierarchy.getPlace().getName());
        }

        if (place.getAlternativeNames() != null
                && !CollectionUtils.isEmpty(place.getAlternativeNames().getAlternativeName())) {

            place.getAlternativeNames().getAlternativeName().stream()
                    .filter(alternativeName -> alternativeName.getName() != null
                            && (NameTypeEnumeration.LABEL.equals(alternativeName.getNameType())
                            || alternativeName.getName().getLang() != null))
                    .forEach(n -> names.add(n.getName()));
        }

        if (up) {
            if (placeHierarchy.getParent() != null) {
                collectNames(placeHierarchy.getParent(), names, up);
            }
        } else {
            if (!CollectionUtils.isEmpty(placeHierarchy.getChildren())) {
                placeHierarchy.getChildren().forEach(child -> collectNames(child, names, up));
            }
        }
    }

    private void populateDocument(StopPlaceHierarchy placeHierarchy, PeliasDocument document) {
        StopPlace place = placeHierarchy.getPlace();

        List<Pair<StopTypeEnumeration, Enum>> stopTypeAndSubModeList = aggregateStopTypeAndSubMode(placeHierarchy);

        document.setCategory(stopTypeAndSubModeList.stream()
                .map(Pair::getLeft).filter(Objects::nonNull)
                .map(StopTypeEnumeration::value)
                .collect(Collectors.toList()));

        if (place.getAlternativeNames() != null
                && !CollectionUtils.isEmpty(place.getAlternativeNames().getAlternativeName())) {
            place.getAlternativeNames().getAlternativeName().stream()
                    .filter(alternativeName ->
                            NameTypeEnumeration.TRANSLATION.equals(alternativeName.getNameType())
                                    && alternativeName.getName() != null && alternativeName.getName().getLang() != null)
                    .forEach(n -> document.addName(n.getName().getLang(), n.getName().getValue()));
        }
        addAlternativeNameLabels(document, placeHierarchy);
        if (document.defaultAlias() == null && document.aliasMap() != null && !document.aliasMap().isEmpty()) {
            String defaultAlias = Optional.of(document.aliasMap().get(DEFAULT_LANGUAGE))
                    .orElse(document.aliasMap().values().iterator().next());
            document.aliasMap().put("default", defaultAlias);
        }

        // Make stop place rank highest in autocomplete by setting popularity
        long popularity = boostConfiguration.getPopularity(stopTypeAndSubModeList, place.getWeighting());
        document.setPopularity(popularity);

        if (place.getTariffZones() != null && place.getTariffZones().getTariffZoneRef() != null) {
            document.setTariffZones(place.getTariffZones().getTariffZoneRef().stream()
                    .map(VersionOfObjectRefStructure::getRef)
                    .collect(Collectors.toList()));


            // A bug in elasticsearch 2.3.4 used for pelias causes prefix queries for array values to fail, thus making it impossible to query by tariff zone prefixes. Instead adding
            // tariff zone authorities as a distinct indexed name.
            document.setTariffZoneAuthorities(place.getTariffZones().getTariffZoneRef().stream()
                    .map(zoneRef -> zoneRef.getRef().split(":")[0]).distinct()
                    .collect(Collectors.toList()));
        }

        // Add parent info locality/county/country
        if (place.getTopographicPlaceRef() != null) {
            Parent parent = document.parent();
            if (parent == null) {
                parent = new Parent();
                document.setParent(parent);
            }
            // TODO: Why only locality, why not county and country ???
            parent.addOrReplaceParentField(
                    Parent.FieldName.LOCALITY,
                    new Parent.Field(place.getTopographicPlaceRef().getRef(), null)
            );
        }
    }

    /**
     * Add alternative names with type 'label' to alias map. Use parents values if not set on stop place.
     */
    private void addAlternativeNameLabels(PeliasDocument document, StopPlaceHierarchy placeHierarchy) {
        StopPlace place = placeHierarchy.getPlace();
        if (place.getAlternativeNames() != null && !CollectionUtils.isEmpty(place.getAlternativeNames().getAlternativeName())) {
            place.getAlternativeNames().getAlternativeName().stream()
                    .filter(alternativeName ->
                            NameTypeEnumeration.LABEL.equals(alternativeName.getNameType())
                                    && alternativeName.getName() != null)
                    .forEach(n -> document.addAlias(
                            Optional.of(n.getName().getLang())
                                    .orElse("default"), n.getName().getValue()));
        }
        if ((document.aliasMap() == null || document.aliasMap().isEmpty()) && placeHierarchy.getParent() != null) {
            addAlternativeNameLabels(document, placeHierarchy.getParent());
        }
    }

    /**
     * Categorize multimodal stops with separate sources in order to be able to filter in queries.
     * <p>
     * Multimodal parents with one layer
     * Multimodal children with another layer
     * Non-multimodal stops with default layer
     *
     * @param hierarchy
     * @return
     */
    private String getLayer(StopPlaceHierarchy hierarchy) {
        if (hierarchy.getParent() != null) {
            return CHILD_STOP_PLACE_LAYER;
        } else if (!CollectionUtils.isEmpty(hierarchy.getChildren())) {
            return PARENT_STOP_PLACE_LAYER;
        }
        return STOP_PLACE_LAYER;
    }

    private List<Pair<StopTypeEnumeration, Enum>> aggregateStopTypeAndSubMode(StopPlaceHierarchy placeHierarchy) {
        List<Pair<StopTypeEnumeration, Enum>> types = new ArrayList<>();

        StopPlace stopPlace = placeHierarchy.getPlace();

        types.add(new ImmutablePair<>(stopPlace.getStopPlaceType(), getStopSubMode(stopPlace)));

        if (!CollectionUtils.isEmpty(placeHierarchy.getChildren())) {
            types.addAll(placeHierarchy.getChildren().stream()
                    .map(this::aggregateStopTypeAndSubMode)
                    .flatMap(Collection::stream).toList());
        }

        return types;
    }

    private Enum getStopSubMode(StopPlace stopPlace) {

        if (stopPlace.getStopPlaceType() != null) {
            switch (stopPlace.getStopPlaceType()) {
                case AIRPORT:
                    return stopPlace.getAirSubmode();
                case HARBOUR_PORT:
                case FERRY_STOP:
                case FERRY_PORT:
                    return stopPlace.getWaterSubmode();
                case BUS_STATION:
                case COACH_STATION:
                case ONSTREET_BUS:
                    return stopPlace.getBusSubmode();
                case RAIL_STATION:
                    return stopPlace.getRailSubmode();
                case METRO_STATION:
                    return stopPlace.getMetroSubmode();
                case ONSTREET_TRAM:
                case TRAM_STATION:
                    return stopPlace.getTramSubmode();
            }
        }
        return null;
    }

    private boolean isQuayLessNonParentStop(StopPlace place) {
        if (place.getQuays() == null || CollectionUtils.isEmpty(place.getQuays().getQuayRefOrQuay())) {
            return place.getKeyList() == null
                    || place.getKeyList().getKeyValue().stream()
                    .noneMatch(
                            kv -> KEY_IS_PARENT_STOP_PLACE.equals(kv.getKey())
                                    && Boolean.TRUE.toString().equalsIgnoreCase(kv.getValue()));
        }
        return false;
    }
}
