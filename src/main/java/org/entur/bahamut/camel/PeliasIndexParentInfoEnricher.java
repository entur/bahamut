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
import org.entur.bahamut.camel.routes.ElasticsearchCommand;
import org.entur.bahamut.camel.routes.json.GeoPoint;
import org.entur.bahamut.camel.routes.json.Parent;
import org.entur.bahamut.camel.routes.json.PeliasDocument;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;

@Service
public class PeliasIndexParentInfoEnricher {

    private static final Logger logger = LoggerFactory.getLogger(PeliasIndexParentInfoEnricher.class);
    public static final String ADMIN_UNITS_CACHE_PROPERTY = "AdminUnitsCache";

    private final GeometryFactory geometryFactory = new GeometryFactory();

    /**
     * Enrich indexing commands with parent info if missing.
     */
    public void addMissingParentInfo(@Body Collection<ElasticsearchCommand> commands,
                                     @ExchangeProperty(value = ADMIN_UNITS_CACHE_PROPERTY) AdminUnitsCache adminUnitRepository) {
        logger.debug("Start updating missing parent info for {} commands", commands.size());
        var ref = new Object() {
            long commandIdx = 1;
        };
        commands.forEach(c -> {
            long startTime = System.nanoTime();
            addMissingParentInfo(c, adminUnitRepository);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            logger.debug("Updated {} / {} command in {} milliseconds", ref.commandIdx, commands.size(), duration);
            ref.commandIdx++;
        });
    }

    @LogExecutionTime
    void addMissingParentInfo(ElasticsearchCommand command, AdminUnitsCache adminUnitRepository) {

        if (!(command.getSource() instanceof PeliasDocument peliasDocument)) {
            return;
        }
        if (isLocalityMissing(peliasDocument.getParent())) {
            long startTime = System.nanoTime();
            addParentIdsByReverseGeoLookup(adminUnitRepository, peliasDocument);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            logger.debug("Locality is missing doing reverseGeoLookup for :" + peliasDocument.getCategory() + " type: " + peliasDocument.getLayer() + "duration(ms): " + duration);
        }

        long startTime = System.nanoTime();
        addAdminUnitNamesByIds(adminUnitRepository, peliasDocument);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;
        logger.debug("addAdminUnitNamesByIds duration(ms): " + duration);
    }

    private void addAdminUnitNamesByIds(AdminUnitsCache adminUnitRepository, PeliasDocument peliasDocument) {
        Parent parent = peliasDocument.getParent();
        if (parent != null) {

            logger.debug("Update parentInfo by Ids");

            if (parent.getLocalityId() != null && parent.getLocality() == null) {
                long startTime = System.nanoTime();
                AdminUnit locality = adminUnitRepository.getLocalityForId(parent.getLocalityId());
                long endTime = System.nanoTime();
                long duration = (endTime - startTime) / 1000000;
                logger.debug("1. Locality is missing get locality name by id: " + parent.getLocalityId() + " type: " + peliasDocument.getLayer() + " duration(ms): " + duration);
                if (locality != null) {
                    parent.setLocality(locality.name());
                    parent.setCountyId(locality.parentId());
                    parent.setCountryId(locality.countryRef());
                } else {
                    // Locality id on document does not match any known locality, match on geography instead
                    long startTime1 = System.nanoTime();
                    addParentIdsByReverseGeoLookup(adminUnitRepository, peliasDocument);
                    long endTime1 = System.nanoTime();
                    long duration1 = (endTime1 - startTime1) / 1000000;
                    logger.debug("2. Locality is still missing ,doing Reverse lookup again:  " + parent.getLocalityId() + " duration: " + duration1);
                    final String adminUnitName = adminUnitRepository.getAdminUnitNameForId(parent.getLocalityId());
                    long duration2 = (endTime1 - startTime1) / 1000000;
                    logger.debug("3. Once again setLocality by Id : " + parent.getLocalityId() + " duration: " + duration2);
                    parent.setLocality(adminUnitName);
                }
            }
            if (parent.getCountyId() != null && parent.getCounty() == null) {
                long startTime = System.nanoTime();
                final String adminUnitName = adminUnitRepository.getAdminUnitNameForId(parent.getCountyId());
                long endTime = System.nanoTime();
                long duration = (endTime - startTime) / 1000000;
                logger.debug("County is missing get county name by id: " + parent.getLocalityId() + " type: " + peliasDocument.getLayer() + " duration(ms): " + duration);
                parent.setCounty(adminUnitName);
            }
        }
    }

    private void addParentIdsByReverseGeoLookup(AdminUnitsCache adminUnitRepository, PeliasDocument peliasDocument) {
        Parent parent = peliasDocument.getParent();

        GeoPoint centerPoint = peliasDocument.getCenterPoint();
        if (centerPoint != null) {
            Point point = geometryFactory.createPoint(new Coordinate(centerPoint.getLon(), centerPoint.getLat()));

            AdminUnit locality = adminUnitRepository.getLocalityForPoint(point);
            if (locality != null) {
                if (parent == null) {
                    parent = new Parent();
                    peliasDocument.setParent(parent);
                }
                parent.setLocalityId(locality.id());
                parent.setCountyId(locality.parentId());
                parent.setCountryId(locality.countryRef());
            } else {
                AdminUnit country = adminUnitRepository.getCountryForPoint(point);
                if (country != null) {
                    if (parent == null) {
                        parent = new Parent();
                        peliasDocument.setParent(parent);
                    }
                    parent.setCountryId(country.countryRef());
                }
            }
        }
    }

    private boolean isLocalityMissing(Parent parent) {
        return parent == null || parent.getLocalityId() == null;
    }

}
