package org.entur.bahamut.routes;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.GenericFile;
import org.entur.bahamut.routes.json.PeliasDocument;
import org.entur.bahamut.routes.mapper.StopPlaceBoostConfiguration;
import org.entur.bahamut.routes.mapper.StopPlaceToPeliasMapper;
import org.entur.bahamut.routes.mapper.TopographicPlaceToPeliasMapper;
import org.entur.netex.NetexParser;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.rutebanken.netex.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class StopPlacesDataRouteBuilder extends RouteBuilder {

    @Value("${stop-places-netex-input-files-path}")
    private static String netexInputPath;

    @Autowired
    private final StopPlaceBoostConfiguration stopPlaceBoostConfiguration;

    public StopPlacesDataRouteBuilder(StopPlaceBoostConfiguration stopPlaceBoostConfiguration) {
        this.stopPlaceBoostConfiguration = stopPlaceBoostConfiguration;
    }

    @Override
    public void configure() throws Exception {

        from("file:/Users/mansoor.sajjad/local-gcs-storage/kakka-dev/tiamat/geocoder?delete=false")
                .process(StopPlacesDataRouteBuilder::parseNetexFile)
                .process(this::fromDeliveryPublicationStructure)
                .bean(new CSVCreator());
    }

    private static void parseNetexFile(Exchange exchange) throws FileNotFoundException {
        var parser = new NetexParser();
        exchange.getIn().setBody(parser.parse(new FileInputStream(exchange.getIn().getBody(GenericFile.class).getAbsoluteFilePath())));
    }

    private void fromDeliveryPublicationStructure(Exchange exchange) {

        NetexEntitiesIndex netexEntitiesIndex = exchange.getIn().getBody(NetexEntitiesIndex.class);

        StopPlaceToPeliasMapper mapper = new StopPlaceToPeliasMapper(stopPlaceBoostConfiguration);

        List<ElasticsearchCommand> stopPlaceCommands = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getStopPlaces().getStopPlace())
                .flatMap(stopPlaces -> toPlaceHierarchies(stopPlaces).stream())
                .map(mapper::toPeliasDocuments)
                .flatMap(Collection::stream)
                .sorted(new PeliasDocumentPopularityComparator()).map(ElasticsearchCommand::peliasIndexCommand).toList();

        List<ElasticsearchCommand> topographicalPlaceCommands = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getTopographicPlaces().getTopographicPlace())
                .flatMap(topographicPlaces -> addTopographicPlaceCommands(topographicPlaces).stream()).toList();

        exchange.getIn().setBody(
                Stream.concat(Stream.of(stopPlaceCommands), Stream.of(topographicalPlaceCommands)).flatMap(List::stream).toList()
        );
    }

    private static List<ElasticsearchCommand> addTopographicPlaceCommands(List<TopographicPlace> places) {
        if (!CollectionUtils.isEmpty(places)) {
            TopographicPlaceToPeliasMapper mapper = new TopographicPlaceToPeliasMapper(1L, Collections.emptyList());
            return places.stream()
                    .map(p -> mapper.toPeliasDocuments(new PlaceHierarchy<>(p)))
                    .flatMap(Collection::stream)
                    .sorted(new PeliasDocumentPopularityComparator())
                    .filter(Objects::nonNull)
                    .map(ElasticsearchCommand::peliasIndexCommand)
                    .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }


    private static class PeliasDocumentPopularityComparator implements Comparator<PeliasDocument> {

        @Override
        public int compare(PeliasDocument o1, PeliasDocument o2) {
            Long p1 = o1 == null || o1.getPopularity() == null ? 1L : o1.getPopularity();
            Long p2 = o2 == null || o2.getPopularity() == null ? 1L : o2.getPopularity();
            return -p1.compareTo(p2);
        }
    }

    /**
     * Map list of stop places to list of hierarchies.
     */
    private static Set<PlaceHierarchy<StopPlace>> toPlaceHierarchies(List<StopPlace> places) {
        Map<String, List<StopPlace>> childrenByParentIdMap = places.stream().filter(sp -> sp.getParentSiteRef() != null).collect(Collectors.groupingBy(sp -> sp.getParentSiteRef().getRef()));
        Set<PlaceHierarchy<StopPlace>> allStopPlaces = new HashSet<>();
        expandStopPlaceHierarchies(places.stream().filter(sp -> sp.getParentSiteRef() == null).map(sp -> createHierarchyForStopPlace(sp, null, childrenByParentIdMap)).collect(Collectors.toList()), allStopPlaces);
        return allStopPlaces;
    }

    private static void expandStopPlaceHierarchies(Collection<PlaceHierarchy<StopPlace>> hierarchies, Set<PlaceHierarchy<StopPlace>> target) {
        if (hierarchies != null) {
            for (PlaceHierarchy<StopPlace> stopPlacePlaceHierarchy : hierarchies) {
                target.add(stopPlacePlaceHierarchy);
                expandStopPlaceHierarchies(stopPlacePlaceHierarchy.getChildren(), target);
            }
        }
    }

    private static PlaceHierarchy<StopPlace> createHierarchyForStopPlace(StopPlace stopPlace, PlaceHierarchy<StopPlace> parent, Map<String, List<StopPlace>> childrenByParentIdMap) {
        List<StopPlace> children = childrenByParentIdMap.get(stopPlace.getId());
        List<PlaceHierarchy<StopPlace>> childHierarchies = new ArrayList<>();
        PlaceHierarchy<StopPlace> hierarchy = new PlaceHierarchy<>(stopPlace, parent);
        if (children != null) {
            childHierarchies = children.stream().map(child -> createHierarchyForStopPlace(child, hierarchy, childrenByParentIdMap)).collect(Collectors.toList());
        }
        hierarchy.setChildren(childHierarchies);
        return hierarchy;
    }
}