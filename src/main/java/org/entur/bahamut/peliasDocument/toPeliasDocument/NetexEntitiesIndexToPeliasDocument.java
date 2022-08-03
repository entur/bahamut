package org.entur.bahamut.peliasDocument.toPeliasDocument;

import org.entur.bahamut.peliasDocument.model.PeliasDocument;
import org.entur.bahamut.peliasDocument.stopPlaceHierarchy.StopPlaceHierarchies;
import org.entur.netex.index.api.NetexEntitiesIndex;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class NetexEntitiesIndexToPeliasDocument {

    public static List<PeliasDocument> map(NetexEntitiesIndex netexEntitiesIndex,
                                           StopPlaceBoostConfiguration stopPlaceBoostConfiguration) {

        var stopPlaceToPeliasDocumentMapper = new StopPlaceHierarchiesToPeliasDocument(stopPlaceBoostConfiguration);
        var topographicPlaceToPeliasDocument = new TopographicPlaceToPeliasDocument(1L);

        var stopPlaceDocuments = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getStopPlaces().getStopPlace())
                .flatMap(stopPlaces -> StopPlaceHierarchies.create(stopPlaces).stream())
                .flatMap(stopPlacePlaceHierarchy -> stopPlaceToPeliasDocumentMapper.toPeliasDocuments(stopPlacePlaceHierarchy).stream())
                .sorted(new NetexEntitiesIndexToPeliasDocument.PeliasDocumentPopularityComparator())
                .toList();

        var topographicalPlaceDocuments = netexEntitiesIndex.getSiteFrames().stream()
                .flatMap(siteFrame -> siteFrame.getTopographicPlaces().getTopographicPlace().stream())
                .flatMap(topographicPlace -> topographicPlaceToPeliasDocument.toPeliasDocuments(topographicPlace).stream())
                .sorted(new NetexEntitiesIndexToPeliasDocument.PeliasDocumentPopularityComparator())
                .toList();


        return Stream.concat(Stream.of(stopPlaceDocuments), Stream.of(topographicalPlaceDocuments))
                .flatMap(List::stream)
                .filter(Objects::nonNull)
                .filter(PeliasDocument::isValid)
                .toList();
    }

    private static class PeliasDocumentPopularityComparator implements Comparator<PeliasDocument> {

        @Override
        public int compare(PeliasDocument o1, PeliasDocument o2) {
            Long p1 = o1 == null || o1.popularity() == null ? 1L : o1.popularity();
            Long p2 = o2 == null || o2.popularity() == null ? 1L : o2.popularity();
            return -p1.compareTo(p2);
        }
    }
}
