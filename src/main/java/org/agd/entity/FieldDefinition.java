package org.agd.entity;

public record FieldDefinition(String fieldName, int size, int byteArrayPosition, double scale, double offset) {
}
