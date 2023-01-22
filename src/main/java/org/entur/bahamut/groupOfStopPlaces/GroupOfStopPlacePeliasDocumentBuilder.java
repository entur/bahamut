package org.entur.bahamut.groupOfStopPlaces;

import org.apache.commons.lang3.StringUtils;
import org.entur.geocoder.model.AddressParts;
import org.entur.geocoder.model.GeoPoint;
import org.entur.geocoder.model.PeliasDocument;
import org.entur.geocoder.model.PeliasId;
import org.rutebanken.netex.model.GroupOfStopPlaces;
import org.rutebanken.netex.model.LocationStructure;
import org.rutebanken.netex.model.MultilingualString;

public class GroupOfStopPlacePeliasDocumentBuilder {

    public static final String DEFAULT_LANGUAGE = "no";

    private final PeliasDocument peliasDocument;

    public GroupOfStopPlacePeliasDocumentBuilder(PeliasId peliasId) {
        this.peliasDocument = new PeliasDocument(peliasId);
    }

    public PeliasDocument build() {
        return peliasDocument;
    }

    public GroupOfStopPlacePeliasDocumentBuilder withDocumentName(MultilingualString name) {
        if (name != null) {
            peliasDocument.setDefaultName(name.getValue());
        }
        return this;
    }

    public GroupOfStopPlacePeliasDocumentBuilder withCategory(String category) {
        peliasDocument.addCategory(category);
        return this;
    }

    public GroupOfStopPlacePeliasDocumentBuilder withPopularity(Long documentPopularity) {
        // Make stop place rank highest in autocomplete by setting popularity
        peliasDocument.setPopularity(documentPopularity);
        return this;
    }


    public GroupOfStopPlacePeliasDocumentBuilder withGroupOfStopPlaces(GroupOfStopPlaces groupOfStopPlaces) {
        return withDescription(groupOfStopPlaces)
                .withCenterPoint(groupOfStopPlaces)
                .withStreetNameToAvoidFalseDuplicates(groupOfStopPlaces);
    }

    private GroupOfStopPlacePeliasDocumentBuilder withDescription(GroupOfStopPlaces groupOfStopPlaces) {
        if (groupOfStopPlaces.getDescription() != null
                && !StringUtils.isEmpty(groupOfStopPlaces.getDescription().getValue())) {
            String lang = groupOfStopPlaces.getDescription().getLang();
            if (lang == null) {
                lang = DEFAULT_LANGUAGE;
            }
            peliasDocument.addDescription(lang, groupOfStopPlaces.getDescription().getValue());
        }
        return this;
    }

    private GroupOfStopPlacePeliasDocumentBuilder withCenterPoint(GroupOfStopPlaces groupOfStopPlaces) {
        if (groupOfStopPlaces.getCentroid() != null) {
            LocationStructure loc = groupOfStopPlaces.getCentroid().getLocation();
            peliasDocument.setCenterPoint(new GeoPoint(loc.getLatitude().doubleValue(), loc.getLongitude().doubleValue()));
        }
        return this;
    }

    /**
     * The Pelias APIs de-duper will throw away results with identical name, layer, parent and address.
     * Setting unique ID in street part of address to avoid unique topographic places with identical
     * names being de-duped.
     * TODO: DO we need this ???
     */
    private GroupOfStopPlacePeliasDocumentBuilder withStreetNameToAvoidFalseDuplicates(GroupOfStopPlaces groupOfStopPlaces) {
        peliasDocument.setAddressParts(new AddressParts("NOT_AN_ADDRESS-" + groupOfStopPlaces.getId()));
        return this;
    }
}
