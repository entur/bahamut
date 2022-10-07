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

package org.entur.bahamut.stopPlaces;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchies;
import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.entur.bahamut.stopPlaces.boostConfiguration.StopPlaceBoostConfiguration;
import org.entur.geocoder.model.*;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.rutebanken.netex.model.*;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.entur.bahamut.stopPlaces.Names.*;
import static org.entur.bahamut.stopPlaces.StopPlaceValidator.isValid;

@Component
public class PeliasDocumentMapper {

    public static final String STOP_PLACE_LAYER = "stop_place";
    public static final String PARENT_STOP_PLACE_LAYER = "stop_place_parent";
    public static final String CHILD_STOP_PLACE_LAYER = "stop_place_child";
    public static final String DEFAULT_LANGUAGE = "no";
    public static final String DEFAULT_SOURCE = "nsr";


    private final StopPlaceBoostConfiguration stopPlaceBoostConfiguration;

    public PeliasDocumentMapper(StopPlaceBoostConfiguration stopPlaceBoostConfiguration) {
        this.stopPlaceBoostConfiguration = stopPlaceBoostConfiguration;
    }

    public Stream<PeliasDocument> toPeliasDocuments(NetexEntitiesIndex netexEntitiesIndex) {

        var stopPlaceToPeliasDocumentMapper = new PeliasDocumentMapper(stopPlaceBoostConfiguration);

        return netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getStopPlaces().getStopPlace())
                .map(StopPlaceHierarchies::create)
                .flatMap(Collection::stream)
                .flatMap(stopPlaceToPeliasDocumentMapper::toPeliasDocumentsForNames)
                .sorted((p1, p2) -> -p1.getPopularity().compareTo(p2.getPopularity()))
                .filter(PeliasDocument::isValid);
    }

    /**
     * TODO: Kan dette gjøres nå ???
     * Map single place hierarchy to (potentially) multiple pelias documents, one per alias/alternative name.
     * Pelias does not yet support queries in multiple languages / for aliases.
     * When support for this is ready this mapping should be refactored to produce
     * a single document per place hierarchy.
     */
    public Stream<PeliasDocument> toPeliasDocumentsForNames(StopPlaceHierarchy placeHierarchy) {
        StopPlace place = placeHierarchy.place();
        if (!isValid(place)) {
            return Stream.empty() ;
        }
        var cnt = new AtomicInteger();

        return getNames(placeHierarchy).stream()
                .map(name -> createPeliasDocument(placeHierarchy, name, cnt.getAndAdd(1)));
    }

    private PeliasDocument createPeliasDocument(StopPlaceHierarchy placeHierarchy, MultilingualString name, int idx) {
        StopPlace place = placeHierarchy.place();

        var idSuffix = idx > 0 ? "-" + idx : "";
        var document = new PeliasDocument(getLayer(placeHierarchy), DEFAULT_SOURCE, place.getId() + idSuffix);

        List<Pair<StopTypeEnumeration, Enum>> stopTypeAndSubModeList = aggregateStopTypeAndSubMode(placeHierarchy);

        setDefaultName(document, name);
        setDisplayName(document, placeHierarchy);
        setCentroid(document, place);
        addIdToStreetNameToAvoidFalseDuplicates(document, place.getId());
        setDescription(document, place);
        setCategories(document, stopTypeAndSubModeList);
        setAlternativeNames(document, place);
        setAlternativeNameLabels(document, placeHierarchy);
        setDefaultAlias(document);
        setPopularity(document, stopPlaceBoostConfiguration, stopTypeAndSubModeList, place);
        setTariffZones(document, place);
        setParent(document, place);

        return document;
    }

    private static void setCentroid(PeliasDocument document, StopPlace place) {
        if (place.getCentroid() != null) {
            var loc = place.getCentroid().getLocation();
            document.setCenterPoint(new GeoPoint(loc.getLatitude().doubleValue(), loc.getLongitude().doubleValue()));
        }
    }

    private static void setDescription(PeliasDocument document, StopPlace place) {
        if (place.getDescription() != null && !StringUtils.isEmpty(place.getDescription().getValue())) {
            var lang = place.getDescription().getLang();
            if (lang == null) {
                lang = DEFAULT_LANGUAGE;
            }
            document.addDescription(lang, place.getDescription().getValue());
        }
    }

    private static void setCategories(PeliasDocument document, List<Pair<StopTypeEnumeration, Enum>> stopTypeAndSubModeList) {
        stopTypeAndSubModeList.stream()
                .map(Pair::getLeft).filter(Objects::nonNull)
                .map(StopTypeEnumeration::value).forEach(document::addCategory);
    }

    /**
     * Make stop place rank highest in autocomplete by setting popularity
     */
    private static void setPopularity(PeliasDocument document,
                                      StopPlaceBoostConfiguration stopPlaceBoostConfiguration,
                                      List<Pair<StopTypeEnumeration, Enum>> stopTypeAndSubModeList,
                                      StopPlace place) {
        long popularity = stopPlaceBoostConfiguration.getPopularity(stopTypeAndSubModeList, place.getWeighting());
        document.setPopularity(popularity);
    }

    private static void setTariffZones(PeliasDocument document, StopPlace place) {
        if (place.getTariffZones() != null && place.getTariffZones().getTariffZoneRef() != null) {
            place.getTariffZones().getTariffZoneRef().stream()
                    .map(VersionOfObjectRefStructure::getRef).forEach(document::addTariffZone);


            // A bug in elasticsearch 2.3.4 used for pelias causes prefix queries for array values to fail,
            // thus making it impossible to query by tariff zone prefixes.
            // Instead, adding tariff zone authorities as a distinct indexed name.
            place.getTariffZones().getTariffZoneRef().stream()
                    .map(zoneRef -> zoneRef.getRef().split(":")[0]).distinct()
                    .forEach(document::addTariffZoneAuthority);
        }
    }

    private static void setParent(PeliasDocument document, StopPlace place) {
        if (place.getTopographicPlaceRef() != null) {
            document.getParents().addOrReplaceParent(
                    ParentType.UNKNOWN,
                    place.getTopographicPlaceRef().getRef(),
                    place.getTopographicPlaceRef().getRef()
            );
        }
    }

    /**
     * Categorize multimodal stops with separate sources in order to be able to filter in queries.
     * <p>
     * Multimodal parents with one layer
     * Multimodal children with another layer
     * Non-multimodal stops with default layer
     *
     */
    private static String getLayer(StopPlaceHierarchy hierarchy) {
        if (hierarchy.parent() != null) {
            return CHILD_STOP_PLACE_LAYER;
        } else if (!CollectionUtils.isEmpty(hierarchy.children())) {
            return PARENT_STOP_PLACE_LAYER;
        }
        return STOP_PLACE_LAYER;
    }

    private static List<Pair<StopTypeEnumeration, Enum>> aggregateStopTypeAndSubMode(StopPlaceHierarchy placeHierarchy) {
        List<Pair<StopTypeEnumeration, Enum>> types = new ArrayList<>();

        StopPlace stopPlace = placeHierarchy.place();

        types.add(new ImmutablePair<>(stopPlace.getStopPlaceType(), getStopSubMode(stopPlace)));

        if (!CollectionUtils.isEmpty(placeHierarchy.children())) {
            types.addAll(placeHierarchy.children().stream()
                    .map(PeliasDocumentMapper::aggregateStopTypeAndSubMode)
                    .flatMap(Collection::stream).toList());
        }

        return types;
    }

    private static Enum getStopSubMode(StopPlace stopPlace) {

        if (stopPlace.getStopPlaceType() != null) {
            switch (stopPlace.getStopPlaceType()) {
                case AIRPORT -> stopPlace.getAirSubmode();
                case HARBOUR_PORT, FERRY_STOP, FERRY_PORT -> stopPlace.getWaterSubmode();
                case BUS_STATION, COACH_STATION, ONSTREET_BUS -> stopPlace.getBusSubmode();
                case RAIL_STATION -> stopPlace.getRailSubmode();
                case METRO_STATION -> stopPlace.getMetroSubmode();
                case ONSTREET_TRAM, TRAM_STATION -> stopPlace.getTramSubmode();
            }
        }
        return null;
    }

    /**
     * The Pelias APIs de-duper will throw away results with identical name, layer, parent and address.
     * Setting unique ID in street part of address to avoid unique topographic places with identical
     * names being de-duped.
     * TODO: DO we need this ???
     */
    private static void addIdToStreetNameToAvoidFalseDuplicates(PeliasDocument document, String placeId) {
        document.setAddressParts(new AddressParts("NOT_AN_ADDRESS-" + placeId));
    }
}
