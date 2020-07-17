/*
 * Copyright (C) 2020 Aiven Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.kafka.connect.common.templating;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;

import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.config.Variables;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public class VariableTemplatePart implements TemplatePart {
    private static final Map<String, DateTimeFormatter> FORMATTER_MAP =
        Map.of(
            "YYYY", DateTimeFormatter.ofPattern("YYYY"),
            "MM", DateTimeFormatter.ofPattern("MM"),
            "dd", DateTimeFormatter.ofPattern("dd"),
            "HH", DateTimeFormatter.ofPattern("HH")
        );

    private final String variableName;

    private final Parameter parameter;

    private final String originalPlaceholder;

    protected VariableTemplatePart(final String originalPlaceholder) {
        this(null, Parameter.EMPTY, originalPlaceholder);
    }

    protected VariableTemplatePart(
        final String variableName,
        final String originalPlaceholder) {
        this(variableName, Parameter.EMPTY, originalPlaceholder);
    }

    protected VariableTemplatePart(
        final String variableName,
        final Parameter parameter,
        final String originalPlaceholder) {
        this.variableName = variableName;
        this.parameter = parameter;
        this.originalPlaceholder = originalPlaceholder;
    }

    public final String variableName() {
        return variableName;
    }

    public final Parameter parameter() {
        return parameter;
    }

    public final String originalPlaceholder() {
        return originalPlaceholder;
    }

    public String format(final SinkRecord sinkRecord) {
        if (FilenameTemplateVariable.KEY.name.equals(variableName)) {
            if (sinkRecord.key() == null) {
                return "null";
            } else if (sinkRecord.keySchema().type() == Schema.Type.STRING) {
                return (String) sinkRecord.key();
            } else {
                return sinkRecord.key().toString();
            }
        }
        return originalPlaceholder;
    }

    public String format(final TimestampSource timestampSource) {
        if (Variables.TIMESTAMP.name.equals(variableName)) {
            return timestampSource.time().format(FORMATTER_MAP.get(parameter.value()));
        }
        if (Variables.UTC_DATE.name.equals(variableName)) {
            return ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE);
        }
        if (Variables.LOCAL_DATE.name.equals(variableName)) {
            return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        }
        return originalPlaceholder;
    }

    public String format(final TimestampSource timestampSource,
                         final String kafkaTopic,
                         final Integer kafkaPartition,
                         final long kafkaOffset) {
        if (Variables.TOPIC.name.equals(variableName)) {
            return kafkaTopic;
        }
        if (Variables.PARTITION.name.equals(variableName)) {
            return kafkaPartition.toString();
        }
        if (Variables.START_OFFSET.name.equals(variableName)) {
            return parameter.asBoolean() ? String.format("%020d", kafkaOffset) : Long.toString(kafkaOffset);
        }
        if (Variables.TIMESTAMP.name.equals(variableName)) {
            return timestampSource.time().format(FORMATTER_MAP.get(parameter.value()));
        }
        if (Variables.UTC_DATE.name.equals(variableName)) {
            return ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE);
        }
        if (Variables.LOCAL_DATE.name.equals(variableName)) {
            return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        }
        return originalPlaceholder;
    }

    public static final class Parameter {

        public static final Parameter EMPTY = new Parameter("__EMPTY__", "__NO_VALUE__");

        private final String name;

        private final String value;

        private Parameter(final String name, final String value) {
            this.name = name;
            this.value = value;
        }

        public static Parameter of(final String name, final String value) {
            if (Objects.isNull(name) && Objects.isNull(value)) {
                return Parameter.EMPTY;
            } else {
                Objects.requireNonNull(name, "name has not been set");
                Objects.requireNonNull(value, "value has not been set");
                return new Parameter(name, value);
            }
        }

        public boolean isEmpty() {
            return this == EMPTY;
        }

        public String name() {
            return name;
        }

        public String value() {
            return value;
        }

        public final Boolean asBoolean() {
            return Boolean.parseBoolean(value);
        }

    }

}
