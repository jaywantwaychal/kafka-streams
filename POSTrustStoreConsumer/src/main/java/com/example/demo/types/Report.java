
package com.example.demo.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "CustomerNumber",
    "TotalLoyaltyPoint"
})
public class Report {

    @JsonProperty("CustomerNumber")
    private String customerNumber;
    @JsonProperty("TotalLoyaltyPoint")
    private Double totalLoyaltyPoint;

    @JsonProperty("CustomerNumber")
    public String getCustomerNumber() {
        return customerNumber;
    }

    @JsonProperty("CustomerNumber")
    public void setCustomerNumber(String customerNumber) {
        this.customerNumber = customerNumber;
    }

    @JsonProperty("TotalLoyaltyPoint")
    public Double getTotalLoyaltyPoint() {
        return totalLoyaltyPoint;
    }

    @JsonProperty("TotalLoyaltyPoint")
    public void setTotalLoyaltyPoint(Double totalLoyaltyPoint) {
        this.totalLoyaltyPoint = totalLoyaltyPoint;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("customerNumber", customerNumber).append("totalLoyaltyPoint", totalLoyaltyPoint).toString();
    }

}
