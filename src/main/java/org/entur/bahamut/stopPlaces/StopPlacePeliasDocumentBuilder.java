package org.entur.bahamut.stopPlaces;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.entur.geocoder.model.*;
import org.rutebanken.netex.model.*;

import java.util.Objects;
import java.util.Optional;

import static org.entur.bahamut.stopPlaces.StopPlaceUtilities.*;

public class StopPlacePeliasDocumentBuilder {

    public static final String DEFAULT_LANGUAGE = "no";

    private final PeliasDocument peliasDocument;

    public StopPlacePeliasDocumentBuilder(PeliasId peliasId) {
        this.peliasDocument = new PeliasDocument(peliasId);
    }

    public PeliasDocument build() {
        return peliasDocument;
    }

    public StopPlacePeliasDocumentBuilder withDocumentName(MultilingualString name) {
        if (name != null) {
            peliasDocument.setDefaultName(name.getValue());
        }
        return this;
    }

    public StopPlacePeliasDocumentBuilder withPopularity(Long documentPopularity) {
        // Make stop place rank highest in autocomplete by setting popularity
        peliasDocument.setPopularity(documentPopularity);
        return this;
    }

    public StopPlacePeliasDocumentBuilder withStopPlaceHierarchy(StopPlaceHierarchy placeHierarchy) {
        return withAlternativeNamesAndAliases(placeHierarchy)
                .withCategories(placeHierarchy)
                .withStopPlace(placeHierarchy.place());
    }

    private StopPlacePeliasDocumentBuilder withAlternativeNamesAndAliases(StopPlaceHierarchy placeHierarchy) {
        return withAlternativeNames(placeHierarchy.place())
                .withAlternativeAliases(placeHierarchy)
                .withDefaultAlias();
    }

    private StopPlacePeliasDocumentBuilder withCategories(StopPlaceHierarchy placeHierarchy) {
        StopTypesSubMode.getStopTypeAndSubMode(placeHierarchy).stream()
                .map(Pair::getLeft).filter(Objects::nonNull)
                .map(StopTypeEnumeration::value)
                .forEach(peliasDocument::addCategory);
        return this;
    }

    private StopPlacePeliasDocumentBuilder withStopPlace(StopPlace place) {
        return withCentroid(place)
                .withDescription(place)
                .withTariffZones(place)
                .withTariffZonesAuthorities(place)
                .withParent(place)
                .withStreetNameToAvoidFalseDuplicates(place);
    }

    private StopPlacePeliasDocumentBuilder withCentroid(StopPlace place) {
        if (place.getCentroid() != null) {
            var loc = place.getCentroid().getLocation();
            peliasDocument.setCenterPoint(new GeoPoint(loc.getLatitude().doubleValue(), loc.getLongitude().doubleValue()));
        }
        return this;
    }

    private StopPlacePeliasDocumentBuilder withAlternativeNames(StopPlace place) {
        findAlternativeNameTranslations(place).forEach(
                alternativeName -> peliasDocument.addAlternativeName(
                        alternativeName.getName().getLang(),
                        alternativeName.getName().getValue()));
        return this;
    }

    private StopPlacePeliasDocumentBuilder withAlternativeAliases(StopPlaceHierarchy placeHierarchy) {
        findAlternativeNameLabels(placeHierarchy).forEach(
                alternativeName -> peliasDocument.addAlternativeAlias(
                        alternativeName.getName().getLang(),
                        alternativeName.getName().getValue()));
        return this;
    }

    private StopPlacePeliasDocumentBuilder withDefaultAlias() {
        if (!peliasDocument.getAlternativeAlias().isEmpty()) {
            String defaultAlias = Optional.of(peliasDocument.getAlternativeAlias().get(DEFAULT_LANGUAGE))
                    .orElse(peliasDocument.getAlternativeAlias().values().iterator().next());
            peliasDocument.setDefaultAlias(defaultAlias);
        }
        return this;
    }

    private StopPlacePeliasDocumentBuilder withDescription(StopPlace place) {
        if (place.getDescription() != null && !StringUtils.isEmpty(place.getDescription().getValue())) {
            var lang = place.getDescription().getLang();
            if (lang == null) {
                lang = DEFAULT_LANGUAGE;
            }
            peliasDocument.addDescription(lang, place.getDescription().getValue());
        }
        return this;
    }

    private StopPlacePeliasDocumentBuilder withTariffZones(StopPlace place) {
        if (place.getTariffZones() != null && place.getTariffZones().getTariffZoneRef() != null) {
            place.getTariffZones().getTariffZoneRef().stream()
                    .map(VersionOfObjectRefStructure::getRef).forEach(peliasDocument::addTariffZone);
        }
        return this;
    }

    private StopPlacePeliasDocumentBuilder withTariffZonesAuthorities(StopPlace place) {
        if (place.getTariffZones() != null && place.getTariffZones().getTariffZoneRef() != null) {
            // TODO: Trenger vi det?
            // A bug in elasticsearch 2.3.4 used for pelias causes prefix queries for array values to fail,
            // thus making it impossible to query by tariff zone prefixes.
            // Instead, adding tariff zone authorities as a distinct indexed name.
            place.getTariffZones().getTariffZoneRef().stream()
                    .map(zoneRef -> zoneRef.getRef().split(":")[0]).distinct()
                    .forEach(peliasDocument::addTariffZoneAuthority);
        }
        return this;
    }

    private StopPlacePeliasDocumentBuilder withParent(StopPlace place) {
        if (place.getTopographicPlaceRef() != null) {
            peliasDocument.getParents().addOrReplaceParent(
                    ParentType.UNKNOWN,
                    PeliasId.of(place.getTopographicPlaceRef().getRef()),
                    place.getTopographicPlaceRef().getRef()
            );
        }
        return this;
    }

    /**
     * The Pelias APIs de-duper will throw away results with identical name, layer, parent and address.
     * Setting unique ID in street part of address to avoid unique topographic places with identical
     * names being de-duped.
     * TODO: DO we need this ???
     */
    private StopPlacePeliasDocumentBuilder withStreetNameToAvoidFalseDuplicates(StopPlace place) {
        peliasDocument.setAddressParts(new AddressParts("NOT_AN_ADDRESS-" + place.getId()));
        return this;
    }
}
