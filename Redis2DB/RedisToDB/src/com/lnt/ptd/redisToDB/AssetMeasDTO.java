package com.lnt.ptd.redisToDB;

public class AssetMeasDTO {
    private final String assetId;
    private final String measurementName;
    private final String measurementType;
    private final String uom;

    public AssetMeasDTO(String assetId, String measurementName, String measurementType, String uom) {
        this.assetId = assetId;
        this.measurementName = measurementName;
        this.measurementType = measurementType;
        this.uom = uom != null ? uom : ""; // Ensure uom is never null
    }

    public String getAssetId() {
        return assetId;
    }

    public String getMeasurementName() {
        return measurementName;
    }

    public String getMeasurementType() {
        return measurementType;
    }

    public String getUom() {
        return uom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AssetMeasDTO that = (AssetMeasDTO) o;

        if (!assetId.equals(that.assetId)) return false;
        if (!measurementName.equals(that.measurementName)) return false;
        if (!measurementType.equals(that.measurementType)) return false;
        return uom.equals(that.uom);
    }

    @Override
    public int hashCode() {
        int result = assetId.hashCode();
        result = 31 * result + measurementName.hashCode();
        result = 31 * result + measurementType.hashCode();
        result = 31 * result + uom.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AssetMeasDTO{" +
                "assetId='" + assetId + '\'' +
                ", measurementName='" + measurementName + '\'' +
                ", measurementType='" + measurementType + '\'' +
                ", uom='" + uom + '\'' +
                '}';
    }
}
