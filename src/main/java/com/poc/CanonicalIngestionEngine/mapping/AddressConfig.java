package com.poc.CanonicalIngestionEngine.mapping;

import java.util.List;

public class AddressConfig {
    private String txnType;
    private String pkRule;
    private List<AddressRule> addresses;

    public String getTxnType() { return txnType; }
    public void setTxnType(String txnType) { this.txnType = txnType; }

    public String getPkRule() { return pkRule; }
    public void setPkRule(String pkRule) { this.pkRule = pkRule; }

    public List<AddressRule> getAddresses() { return addresses; }
    public void setAddresses(List<AddressRule> addresses) { this.addresses = addresses; }
}
