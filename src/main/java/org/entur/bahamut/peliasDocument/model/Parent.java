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

package org.entur.bahamut.peliasDocument.model;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.*;

public class Parent {

    private final Map<FieldName, Field> fields = new HashMap<>();

    public void addOrReplaceParentField(FieldName fieldName, Field field) {
        fields.compute(fieldName, (fldName, fld) -> field);
    }

    public void setNameFor(FieldName fieldName, String name) {
        fields.computeIfPresent(fieldName, (fldName, field) -> new Field(field.id(), name, field.abbreviation(), field.source()));
    }

    public Optional<String> idFor(Parent.FieldName fieldName) {
        return Optional.ofNullable(fields.get(fieldName)).map(Parent.Field::id);
    }

    public Optional<String> nameFor(Parent.FieldName fieldName) {
        return Optional.ofNullable(fields.get(fieldName)).map(Parent.Field::name);
    }

    public Map<FieldName, Field> getParentFields() {
        return fields;
    }

    public record Field(String id, String name, String abbreviation, String source) {
        public Field(String id, String name) {
            this(id, name, null, null);
        }
    }

    public enum FieldName {
        COUNTRY("country"),
        COUNTY("county"),
        LOCALITY("locality");

        private final String value;

        FieldName(String value) {
            this.value = value;
        }

        @JsonValue
        public String value() {
            return value;
        }
    }
}