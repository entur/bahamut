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

package org.entur.bahamut.camel;

import org.apache.camel.Body;
import org.apache.camel.ExchangeProperty;
import org.entur.bahamut.camel.adminUnitsRepository.AdminUnit;
import org.entur.bahamut.camel.adminUnitsRepository.AdminUnitsCache;
import org.entur.bahamut.camel.routes.json.GeoPoint;
import org.entur.bahamut.camel.routes.json.Parent;
import org.entur.bahamut.camel.routes.json.PeliasDocument;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

@Service
public class PeliasIndexParentInfoEnricher {

    private static final Logger logger = LoggerFactory.getLogger(PeliasIndexParentInfoEnricher.class);
    public static final String ADMIN_UNITS_CACHE_PROPERTY = "AdminUnitsCache";

    private final GeometryFactory geometryFactory = new GeometryFactory();

    /**
     * Enrich indexing peliasDocuments with parent info if missing.
     */
    public void addMissingParentInfo(@Body List<PeliasDocument> peliasDocuments,
                                     @ExchangeProperty(value = ADMIN_UNITS_CACHE_PROPERTY) AdminUnitsCache adminUnitCache) {
        logger.debug("Start updating missing parent info for {} peliasDocuments", peliasDocuments.size());

        AtomicInteger index = new AtomicInteger(0);
        peliasDocuments
                .forEach(peliasDocument -> logExecutionTimeFor(
                                this::addMissingParentInfo,
                                "Updated" + index.incrementAndGet() + " / " + peliasDocuments.size() + " command"
                        ).accept(adminUnitCache, peliasDocument)
                );
    }

    private void addMissingParentInfo(AdminUnitsCache adminUnitsCache, PeliasDocument peliasDocument) {
        Parent parent = peliasDocument.getParent();

        if (parent == null || parent.idFor(Parent.FieldName.LOCALITY).isEmpty()) {
            logExecutionTimeFor(
                    this::addParentInfoByReverseGeoLookup,
                    "Locality is missing doing reverseGeoLookup for :" + peliasDocument.getCategory() + " type: " + peliasDocument.getLayer()
            ).accept(adminUnitsCache, peliasDocument);
        }

        // if we were able to add the localityId in the previous step, or it was already there.
        if (parent != null) {
            parent.idFor(Parent.FieldName.LOCALITY).ifPresent(localityId ->
                    logExecutionTimeFor(
                            this::addParentInfoByIds,
                            "LocalityId exists, adding parent info"
                    ).accept(adminUnitsCache, peliasDocument)
            );
        }
    }

    private void addParentInfoByIds(AdminUnitsCache adminUnitsCache, PeliasDocument peliasDocument) {
        Parent parent = peliasDocument.getParent();

        parent.idFor(Parent.FieldName.LOCALITY).ifPresent(localityId -> {
            if (parent.nameFor(Parent.FieldName.LOCALITY).isEmpty()) {
                AdminUnit adminUnitLocality = logExecutionTimeFor(
                        () -> adminUnitsCache.getLocalityForId(localityId),
                        "1. Locality is missing get locality name by id: " + localityId + " type: " + peliasDocument.getLayer()
                ).get();
                if (adminUnitLocality != null) {
                    parent.setNameFor(Parent.FieldName.LOCALITY, adminUnitLocality.name());
                    parent.addOrReplaceParentField(Parent.FieldName.COUNTY, new Parent.Field(adminUnitLocality.parentId()));
                    parent.addOrReplaceParentField(Parent.FieldName.COUNTRY, new Parent.Field(adminUnitLocality.countryRef()));
                } else {
                    // Locality id on document does not match any known locality, match on geography instead
                    logExecutionTimeFor(
                            this::addParentInfoByReverseGeoLookup,
                            "Locality is missing doing reverseGeoLookup for :" + peliasDocument.getCategory() + " type: " + peliasDocument.getLayer()
                    ).accept(adminUnitsCache, peliasDocument);

                    logExecutionTimeFor(
                            this::addParentInfoByReverseGeoLookup,
                            "2. Locality is still missing ,doing Reverse lookup again:  " + localityId
                    ).accept(adminUnitsCache, peliasDocument);

                    String adminUnitName = logExecutionTimeFor(
                            () -> adminUnitsCache.getAdminUnitNameForId(localityId),
                            "3. Once again setLocality by Id : " + localityId
                    ).get();

                    parent.setNameFor(Parent.FieldName.LOCALITY, adminUnitName);
                }
            }
        });


        parent.idFor(Parent.FieldName.COUNTY).ifPresent(countyId -> {
            if (parent.nameFor(Parent.FieldName.COUNTY).isEmpty()) {
                String adminUnitName = logExecutionTimeFor(
                        () -> adminUnitsCache.getAdminUnitNameForId(countyId),
                        "County is missing get county name by id: " + countyId + " type: " + peliasDocument.getLayer()
                ).get();
                parent.setNameFor(Parent.FieldName.COUNTY, adminUnitName);
            }
        });
    }

    private void addParentInfoByReverseGeoLookup(AdminUnitsCache adminUnitRepository, PeliasDocument peliasDocument) {
        GeoPoint centerPoint = peliasDocument.getCenterPoint();
        if (centerPoint != null) {
            Point point = geometryFactory.createPoint(new Coordinate(centerPoint.lon(), centerPoint.lat()));

            AdminUnit adminUnitLocality = adminUnitRepository.getLocalityForPoint(point);
            AdminUnit adminUnitCountry = adminUnitRepository.getCountryForPoint(point); // TODO: No need to run it, if its not needed
            Parent parent = peliasDocument.getParent();
            if (adminUnitLocality != null) {
                if (parent == null) {
                    parent = new Parent();
                    peliasDocument.setParent(parent);
                }
                parent.addOrReplaceParentField(Parent.FieldName.LOCALITY, new Parent.Field(adminUnitLocality.id()));
                parent.addOrReplaceParentField(Parent.FieldName.COUNTY, new Parent.Field(adminUnitLocality.parentId()));
                parent.addOrReplaceParentField(Parent.FieldName.COUNTRY, new Parent.Field(adminUnitLocality.countryRef()));
            } else if (adminUnitCountry != null) {
                if (parent == null) {
                    parent = new Parent();
                    peliasDocument.setParent(parent);
                }
                parent.addOrReplaceParentField(Parent.FieldName.COUNTRY, new Parent.Field(adminUnitCountry.countryRef()));
            }
        }
    }

    private BiConsumer<AdminUnitsCache, PeliasDocument> logExecutionTimeFor(BiConsumer<AdminUnitsCache, PeliasDocument> consumer, String logStatement) {
        return (adminUnitsCache, peliasDocument) -> {
            long startTime = System.nanoTime();
            consumer.accept(adminUnitsCache, peliasDocument);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            logger.debug(logStatement + " duration(ms): " + duration);
        };
    }

    private <T> Supplier<T> logExecutionTimeFor(Supplier<T> supplier, String logStatement) {
        return () -> {
            long startTime = System.nanoTime();
            T value = supplier.get();
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            logger.debug(logStatement + " duration(ms): " + duration);
            return value;
        };
    }
}
