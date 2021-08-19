package com.cs.rfq.decorator;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;

public class Rfq implements Serializable {
    private String id;
    private String isin;
    private Long traderId;
    private Long entityId;
    private Long quantity;
    private Double price;
    private String side;

    public static Rfq fromJson(String json) {
        JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
        Rfq RfqObject = new Rfq();
        RfqObject.setId(jsonObject.get("id").getAsString());
        RfqObject.setIsin(jsonObject.get("instrumentId").getAsString());
        RfqObject.setTraderId(jsonObject.get("traderId").getAsLong());
        RfqObject.setEntityId(jsonObject.get("entityId").getAsLong());
        RfqObject.setQuantity(jsonObject.get("qty").getAsLong());
        RfqObject.setPrice(jsonObject.get("price").getAsDouble());
        RfqObject.setSide(jsonObject.get("side").getAsString());
        return RfqObject;
    }

    @Override
    public String toString() {
        return "Rfq{" +
                "id='" + id + '\'' +
                ", isin='" + isin + '\'' +
                ", traderId=" + traderId +
                ", entityId=" + entityId +
                ", quantity=" + quantity +
                ", price=" + price +
                ", side=" + side +
                '}';
    }

    public boolean isBuySide() {
        return "B".equals(side);
    }

    public boolean isSellSide() {
        return "S".equals(side);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIsin() {
        return isin;
    }

    public void setIsin(String isin) {
        this.isin = isin;
    }

    public Long getTraderId() {
        return traderId;
    }

    public void setTraderId(Long traderId) {
        this.traderId = traderId;
    }

    public Long getEntityId() {
        return entityId;
    }

    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }
}
