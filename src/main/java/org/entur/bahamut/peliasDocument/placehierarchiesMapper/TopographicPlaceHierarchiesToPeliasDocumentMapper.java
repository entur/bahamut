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

package org.entur.bahamut.peliasDocument.placehierarchiesMapper;

import org.entur.bahamut.peliasDocument.placeHierarchy.PlaceHierarchy;
import org.entur.bahamut.peliasDocument.model.PeliasDocument;
import org.rutebanken.netex.model.MultilingualString;
import org.rutebanken.netex.model.TopographicPlace;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class TopographicPlaceHierarchiesToPeliasDocumentMapper extends PlaceHierarchiesToPeliasDocumentMapper<TopographicPlace> {

    private final long popularity;

    public TopographicPlaceHierarchiesToPeliasDocumentMapper(long popularity) {
        super();
        this.popularity = popularity;
    }

    @Override
    protected void populateDocument(PlaceHierarchy<TopographicPlace> placeHierarchy, PeliasDocument document) {
        TopographicPlace place = placeHierarchy.getPlace();
        if (place.getAlternativeDescriptors() != null && !CollectionUtils.isEmpty(place.getAlternativeDescriptors().getTopographicPlaceDescriptor())) {
            place.getAlternativeDescriptors().getTopographicPlaceDescriptor().stream()
                    .filter(descriptor -> descriptor.getName() != null && descriptor.getName().getLang() != null)
                    .forEach(descriptor -> document.addName(descriptor.getName().getLang(), descriptor.getName().getValue()));
        }
        document.setPopularity(popularity);
    }

    @Override
    protected MultilingualString getDisplayName(PlaceHierarchy<TopographicPlace> placeHierarchy) {
        TopographicPlace place = placeHierarchy.getPlace();
        if (place.getName() != null) {
            return placeHierarchy.getPlace().getName();
        }    // Use descriptor.name if name is not set
        else if (place.getDescriptor() != null && place.getDescriptor().getName() != null) {
            return place.getDescriptor().getName();
        }
        return null;
    }

    @Override
    protected List<MultilingualString> getNames(PlaceHierarchy<TopographicPlace> placeHierarchy) {
        List<MultilingualString> names = new ArrayList<>();
        TopographicPlace place = placeHierarchy.getPlace();

        MultilingualString displayName = getDisplayName(placeHierarchy);
        if (displayName != null) {
            names.add(displayName);
        }

        if (place.getAlternativeDescriptors() != null && !CollectionUtils.isEmpty(place.getAlternativeDescriptors().getTopographicPlaceDescriptor())) {
            place.getAlternativeDescriptors().getTopographicPlaceDescriptor().stream().filter(an -> an.getName() != null && an.getName().getLang() != null).forEach(n -> names.add(n.getName()));
        }
        return PlaceHierarchiesMapperUtilities.filterUnique(names);
    }

    @Override
    protected String getLayer(TopographicPlace place) {
        return switch (place.getTopographicPlaceType()) {
            case MUNICIPALITY -> "locality";
            case COUNTY -> "county";
            default -> null;
        };
    }
}