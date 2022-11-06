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

package org.entur.bahamut.stopPlaces.boostConfiguration;

import org.apache.commons.lang3.tuple.Pair;
import org.entur.bahamut.stopPlaces.StopTypesSubMode;
import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.rutebanken.netex.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class StopPlaceBoostConfiguration {

    private static final String ALL_TYPES = "*";
    private final Map<StopTypeEnumeration, StopTypeBoostConfig> stopTypeFactorMap = new HashMap<>();
    private final Map<InterchangeWeightingEnumeration, Double> interchangeFactorMap = new HashMap<>();
    private final long defaultValue;

    @Autowired
    public StopPlaceBoostConfiguration(@Value("${pelias.stop.place.boost.config:{\"defaultValue\":1000}}") String boostConfig) {

        StopPlaceBoostConfigJSON input = StopPlaceBoostConfigJSON.fromString(boostConfig);

        this.defaultValue = input.defaultValue;

        if (input.interchangeFactors != null) {
            initiateInterchangeFactors(input.interchangeFactors);
        }

        if (input.stopTypeFactors != null) {
            initiateStopTypeFactors(input.stopTypeFactors);
        }
    }

    public long getPopularity(StopPlaceHierarchy placeHierarchy) {
        long popularity = defaultValue;

        double stopTypeAndSubModeFactor = StopTypesSubMode.getStopTypeAndSubMode(placeHierarchy).stream()
                .collect(Collectors.summarizingDouble(stopTypeAndSubMode ->
                        getStopTypeAndSubModeFactor(stopTypeAndSubMode.getLeft(), stopTypeAndSubMode.getRight())))
                .getSum();

        if (stopTypeAndSubModeFactor > 0) {
            popularity *= stopTypeAndSubModeFactor;
        }

        Double interchangeFactor = interchangeFactorMap.get(placeHierarchy.place().getWeighting());
        if (interchangeFactor != null) {
            popularity *= interchangeFactor;
        }

        return popularity;
    }

    private void initiateInterchangeFactors(Map<String, Double> interchangeFactors) {
        interchangeFactors.forEach((interchangeType, factor) ->
                interchangeFactorMap.put(
                        InterchangeWeightingEnumeration.fromValue(interchangeType),
                        factor)
        );
    }

    private void initiateStopTypeFactors(Map<String, Map<String, Double>> stopTypeFactors) {
        for (Map.Entry<String, Map<String, Double>> stopTypeConfig : stopTypeFactors.entrySet()) {

            StopTypeEnumeration stopType = StopTypeEnumeration.fromValue(stopTypeConfig.getKey());
            Map<String, Double> inputFactorsPerSubMode = stopTypeConfig.getValue();

            StopTypeBoostConfig stopTypeBoostConfig =
                    new StopTypeBoostConfig(inputFactorsPerSubMode.getOrDefault(ALL_TYPES, 1.0));
            stopTypeFactorMap.put(stopType, stopTypeBoostConfig);

            inputFactorsPerSubMode.remove(ALL_TYPES);
            inputFactorsPerSubMode
                    .forEach((subModeString, factor) ->
                            stopTypeBoostConfig.factorPerSubMode().put(
                                    toSubModeEnum(stopType, subModeString),
                                    factor
                            )
                    );
        }
    }

    private double getStopTypeAndSubModeFactor(StopTypeEnumeration stopType, Enum subMode) {
        StopTypeBoostConfig factorsPerSubMode = stopTypeFactorMap.get(stopType);
        if (factorsPerSubMode != null) {
            return factorsPerSubMode.getFactorForSubMode(subMode);
        }
        return 0;
    }

    private Enum toSubModeEnum(StopTypeEnumeration stopType, String subMode) {
        return switch (stopType) {
            case AIRPORT -> AirSubmodeEnumeration.fromValue(subMode);
            case HARBOUR_PORT, FERRY_STOP, FERRY_PORT -> WaterSubmodeEnumeration.fromValue(subMode);
            case BUS_STATION, COACH_STATION, ONSTREET_BUS -> BusSubmodeEnumeration.fromValue(subMode);
            case RAIL_STATION -> RailSubmodeEnumeration.fromValue(subMode);
            case METRO_STATION -> MetroSubmodeEnumeration.fromValue(subMode);
            case ONSTREET_TRAM, TRAM_STATION -> TramSubmodeEnumeration.fromValue(subMode);
            default -> null;
        };
    }

    private record StopTypeBoostConfig(
            double defaultFactor,
            Map<Enum, Double> factorPerSubMode
    ) {
        public StopTypeBoostConfig(double defaultFactor) {
            this(defaultFactor, new HashMap<>());
        }

        public Double getFactorForSubMode(Enum subMode) {
            return factorPerSubMode.getOrDefault(subMode, defaultFactor);
        }
    }
}