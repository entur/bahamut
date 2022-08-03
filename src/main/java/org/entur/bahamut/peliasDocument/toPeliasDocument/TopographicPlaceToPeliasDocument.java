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
import org.entur.bahamut.peliasDocument.model.PeliasDocument;
import org.rutebanken.netex.model.MultilingualString;
import org.rutebanken.netex.model.TopographicPlace;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TopographicPlaceToPeliasDocument {

    private final long popularity;

    public TopographicPlaceToPeliasDocument(long popularity) {
        this.popularity = popularity;
    }

    /**
     * Map single place hierarchy to (potentially) multiple pelias documents, one per alias/alternative name.
     * <p>
     * Pelias does not yet support queries in multiple languages / for aliases. When support for this is ready this mapping should be
     * refactored to produce a single document per place hierarchy.
     */
    public List<PeliasDocument> toPeliasDocuments(TopographicPlace place) {
        if (!isValid(place)) {
            return new ArrayList<>();
        }
        var cnt = new AtomicInteger();

        return getNames(place).stream()
                .map(name -> toPeliasDocument(place, name, cnt.getAndAdd(1)))
                .collect(Collectors.toList());
    }

    private PeliasDocument toPeliasDocument(TopographicPlace place, MultilingualString name, int idx) {

        var idSuffix = idx > 0 ? "-" + idx : "";

        var document = new PeliasDocument(getLayer(place), place.getId() + idSuffix);
        if (name != null) {
            document.setDefaultNameAndPhrase(name.getValue());
        }

        // Add official name as display name.
        // Not a part of standard pelias model, will be copied to name.default before deduping and labelling in Entur-pelias API.
        // TODO: What is this ???
        var displayName = place.getName();
        if (displayName != null) {
            document.getNameMap().put("display", displayName.getValue());
            if (displayName.getLang() != null) {
                document.addName(displayName.getLang(), displayName.getValue());
            }
        }

        // TopographicPlaces
        if (place.getPolygon() != null) {
            // TODO issues with shape validation in elasticsearch. duplicate coords + intersections cause document to be discarded. is shape even used by pelias?
            document.setShape(ToPeliasDocumentUtilities.toPolygon(place.getPolygon().getExterior().getAbstractRing().getValue()));
        }

        ToPeliasDocumentUtilities.addIdToStreetNameToAvoidFalseDuplicates(place.getId(), document);

        if (place.getDescription() != null && !StringUtils.isEmpty(place.getDescription().getValue())) {
            var lang = place.getDescription().getLang();
            if (lang == null) {
                lang = ToPeliasDocumentUtilities.DEFAULT_LANGUAGE;
            }
            document.addDescription(lang, place.getDescription().getValue());
        }

        populateDocument(place, document);
        return document;
    }

    protected boolean isValid(TopographicPlace place) {
        var layer = getLayer(place);

        if (layer == null) {
            return false;
        }
        return ToPeliasDocumentUtilities.isValid(place);
    }

    protected void populateDocument(TopographicPlace place, PeliasDocument document) {
        if (place.getAlternativeDescriptors() != null && !CollectionUtils.isEmpty(place.getAlternativeDescriptors().getTopographicPlaceDescriptor())) {
            place.getAlternativeDescriptors().getTopographicPlaceDescriptor().stream()
                    .filter(descriptor -> descriptor.getName() != null && descriptor.getName().getLang() != null)
                    .forEach(descriptor -> document.addName(descriptor.getName().getLang(), descriptor.getName().getValue()));
        }
        document.setPopularity(popularity);
    }

    protected MultilingualString getDisplayName(TopographicPlace place) {
        if (place.getName() != null) {
            return place.getName();
        }
        // Use descriptor.name if name is not set
        else if (place.getDescriptor() != null && place.getDescriptor().getName() != null) {
            return place.getDescriptor().getName();
        }
        return null;
    }

    protected List<MultilingualString> getNames(TopographicPlace place) {
        List<MultilingualString> names = new ArrayList<>();

        MultilingualString displayName = getDisplayName(place);
        if (displayName != null) {
            names.add(displayName);
        }

        if (place.getAlternativeDescriptors() != null && !CollectionUtils.isEmpty(place.getAlternativeDescriptors().getTopographicPlaceDescriptor())) {
            place.getAlternativeDescriptors().getTopographicPlaceDescriptor().stream().filter(an -> an.getName() != null && an.getName().getLang() != null).forEach(n -> names.add(n.getName()));
        }
        return ToPeliasDocumentUtilities.filterUnique(names);
    }

    protected String getLayer(TopographicPlace place) {
        return switch (place.getTopographicPlaceType()) {
            case MUNICIPALITY -> "locality";
            case COUNTY -> "county";
            default -> null;
        };
    }
}