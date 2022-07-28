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

package org.entur.bahamut.camel.adminUnitsRepository;

import net.opengis.gml._3.AbstractRingPropertyType;
import net.opengis.gml._3.DirectPositionListType;
import net.opengis.gml._3.LinearRingType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.rutebanken.netex.model.TopographicPlace;
import org.rutebanken.netex.model.TopographicPlaceTypeEnumeration;

import javax.xml.bind.JAXBElement;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public record AdminUnit(
        String id,
        String isoCode,
        String parentId,
        String name,
        String countryRef,
        Polygon geometry,
        AdminUnitType adminUnitType
) {

    public static AdminUnit makeAdminUnit(TopographicPlace topographicPlace) {
        return new AdminUnit(
                topographicPlace.getId(),
                topographicPlace.getIsoCode(),
                topographicPlace.getParentTopographicPlaceRef() != null ? topographicPlace.getParentTopographicPlaceRef().getRef() : null,
                topographicPlace.getDescriptor().getName().getValue(),
                topographicPlace.getCountryRef().getRef().name(),
                new GeometryFactory().createPolygon(convertToCoordinateSequence(topographicPlace.getPolygon().getExterior())),
                getAdminUnitType(topographicPlace.getTopographicPlaceType())
        );
    }

    public String getISO3CountryName() {
        return new Locale("en", countryRef).getISO3Country();
    }

    private static CoordinateSequence convertToCoordinateSequence(AbstractRingPropertyType abstractRingPropertyType) {
        var coordinateValues = Optional.of(abstractRingPropertyType)
                .map(AbstractRingPropertyType::getAbstractRing)
                .map(JAXBElement::getValue)
                .map(abstractRing -> ((LinearRingType) abstractRing))
                .map(LinearRingType::getPosList)
                .map(DirectPositionListType::getValue).orElse(Collections.emptyList());

        var coordinates = new Coordinate[coordinateValues.size() / 2];
        int coordinateIndex = 0;
        for (int index = 0; index < coordinateValues.size(); index += 2) {
            var coordinate = new Coordinate(coordinateValues.get(index + 1), coordinateValues.get(index));
            coordinates[coordinateIndex++] = coordinate;
        }
        return new CoordinateArraySequence(coordinates);
    }

    private static AdminUnitType getAdminUnitType(TopographicPlaceTypeEnumeration topographicPlaceType) {
        return switch (topographicPlaceType) {
            case COUNTRY -> AdminUnitType.COUNTRY;
            case COUNTY -> AdminUnitType.COUNTY;
            case MUNICIPALITY -> AdminUnitType.LOCALITY;
            default -> null;
        };
    }
}
