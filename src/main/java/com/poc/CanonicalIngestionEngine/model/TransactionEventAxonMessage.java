package com.poc.CanonicalIngestionEngine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionEventAxonMessage {

    // =====================================================
    // ACCOUNT & CARD
    // =====================================================
    private String acctNum;
    private String acctType;
    private String accountHolderName;
    private String accountUri;
    private String accountInformationId;
    private String accountValidationId;
    private String senderAccountNumberType;
    private String receiverAccountNumberType;
    private String paymentAccountReference;

    // =====================================================
    // ACQUIRING
    // =====================================================
    private String acqIca;
    private String acquiringBin;
    private String acquirerReferenceId;
    private String acqReferenceText;
    private String acqRefTxt;
    private String acquiringCountry;
    private String acquiringCountryCd;
    private String acquiringIdentificationCd;
    private String acquiringCredentialId;
    private String acquiringPrcssrId;
    private String acquiringInstitutionId;
    private String visaAcquiringBin;

    // =====================================================
    // TRANSACTION CORE
    // =====================================================
    private String tranId;
    private String tranAmt;
    private String tranAmtCurr;
    private String tranAmtExpnt;
    private String tranAmtCurrNumeric;
    private String tranTypeCd;
    private String tranTypeInd;
    private String tranProcessCd;
    private String tranOrigCountryCd;
    private String tranOrigInstId;
    private String transactionCategoryCode;
    private String transactionLocalDateTime;
    private String transactionLocalDateTimestamp;
    private String transactionOriginationIdentifier;
    private String transactionId;
    private String transactionPurpose;
    private String transactionLinkId;
    private String tranClearDtlId;

    // =====================================================
    // NETWORK
    // =====================================================
    private String networkCode;
    private String networkGateway;
    private String origNetworkGateway;
    private String networkReceiveTimeStamp;
    private String networkResponseCode;
    private String networkResponseCodeDesc;
    private String networkSendTimeStamp;
    private String ntwrkRefNum;
    private String networkReferenceNum;
    private String ntwrkStatDesc;
    private String processedNetwork;

    // =====================================================
    // STATUS & RESPONSE
    // =====================================================
    private String status;
    private String statusReason;
    private String statusTimestamp;
    private String internalStatus;
    private String fundsAvailable;
    private String fundAvailable;
    private String funcCode;
    private String authIdResp;
    private String origNtwrkRespCd;
    private String networkDecisionCode;

    // =====================================================
    // SENDER (SNDR)
    // =====================================================
    private String sndrAcct;
    private String sndrAcctUri;
    private String sndrCardExpirDt;
    private String sndrAddrLine1;
    private String sndrAddrLine2;
    private String sndrBirthCountry;
    private String sndrBirthDt;
    private String sndrCityName;
    private String sndrCountryCd;
    private String sndrCountrySubCd;
    private String sndrDigitalAcctRefNum;
    private String sndrEmailAddr;
    private String sndrFirstName;
    private String sndrLastName;
    private String sndrMiddleName;
    private String sndrNationalityCd;
    private String sndrPaymentFacilitatorId;
    private String sndrSubMerchantId;
    private String sndrPhoneNum;
    private String sndrPostalCd;
    private String sndrSanctionScore;
    private String sndrFromAccount;
    private String sndrOrigName;
    private String senderAliasTypeCd;
    private String senderAliasValTxt;
    private String senderErrorReasonCode;
    private String senderErrorReasonCodeDescription;
    private String senderResponseReasonCode;
    private String senderResponseReasonDetail;
    private int senderEligible;
    private List<GovtId> sndrGovtId;

    // =====================================================
    // RECEIVER (RCVR)
    // =====================================================
    private String rcvrAcct;
    private String rcvrAcctUri;
    private String rcvrAddrLine1;
    private String rcvrAddrLine2;
    private String rcvrBirthCountry;
    private String rcvrBirthDate;
    private String rcvrCardExpirDt;
    private String rcvrCity;
    private String rcvrCountryCd;
    private String rcvrCountrySubCd;
    private String rcvrEmailAddr;
    private String rcvrExternalAcctRefId;
    private String rcvrExternalRefId;
    private String rcvrFirstName;
    private String rcvrLastName;
    private String rcvrMiddleName;
    private String rcvrNationalityCd;
    private String rcvrPaymentFacilitatorId;
    private String rcvrPhoneNum;
    private String rcvrPostalCd;
    private String rcvrSanctionScore;
    private String rcvrSubMerchantId;
    private String rcvrTokenValue;
    private String rcvrOrigName;
    private String rcvrAliasTypeCd;
    private String rcvrAliasValue;
    private String receiverErrorReasonCode;
    private String receiverErrorReasonCodeDescription;
    private String receiverResponseReasonCode;
    private String receiverResponseReasonDetail;
    private int receiverEligible;
    private List<GovtId> rcvrGovtId;

    // =====================================================
    // CARD ACCEPTOR / MERCHANT
    // =====================================================
    private String cardAcceptorAddrCity;
    private String cardAcceptorAddrCountry;
    private String cardAcceptorAddrPostalCd;
    private String cardAcceptorAddrState;
    private String cardAcceptorAddrStreet;
    private String cardAcceptorIdCd;
    private String cardAcceptorName;
    private String cardAcceptorTerminalId;
    private String cardTypeCode;
    private String merchantType;
    private String merchantCategoryCd;
    private String merchantAdviceCode;
    private String merchantVerificationValue;
    private String merchantVerifyValue;
    private String acceptorTaxId;
    private String acceptorTaxIdName;

    // =====================================================
    // TRANSFER ACCEPTOR
    // =====================================================
    private String transferAccptName;
    private String transferAccptId;
    private String transferAccptTerminalId;
    private String transferAccptAddrLn1;
    private String transferAccptAddrCity;
    private String transferAccptAddrState;
    private String transferAccptAddrCntry;
    private String transferAccptAddrPostalCd;
    private String transferAcceptorConvenienceAmt;
    private String transferAcceptorConvenienceIndicator;
    private String transferAcceptorPhoneNum;
    private String transferAcceptorMpgId;

    // =====================================================
    // TRANSFER / FUNDING
    // =====================================================
    private String transferId;
    private String transferRef;
    private String transferFailSrcCd;
    private String fundingTranId;
    private String fundingId;
    private String fundingRef;
    private String fundingSource;
    private String mcFundingSource;
    private String fundingStatus;
    private String mappedFundingSource;
    private String fundingIchgFee;

    // =====================================================
    // PAYMENT LEG
    // =====================================================
    private String paymentStatus;
    private String paymentAmt;
    private String paymentCurrCd;
    private String paymentSetlAmt;
    private String paymentSetlCurrCd;
    private String paymentStatusReason;
    private String paymentNetworkCode;
    private String paymentTranProcessCd;
    private String paymentMsgTypeInd;
    private String paymentTransactionCategoryCode;
    private String paymentOrigNtwrkRespCd;
    private String paymentMerchantType;
    private String paymentIchgRateDsgnCd;
    private String paymentNetworkResponseCode;
    private String paymentNetworkResponseDesc;
    private String paymentSysTraceAudNum;
    private String paymentRetrievalRefNum;
    private String paymentSwitchSerialNumber;
    private String paymentUnqRefNum;
    private String paymentAuthIdResp;
    private String paymentNtwrkRefNum;
    private String paymentNetworkSendTimeStamp;
    private String paymentNetworkReceiveTimeStamp;
    private String paymentFundsAvailable;
    private String paymentRef;
    private String paymentTranTypeCd;
    private String paymentId;
    private String paymentQrData;

    // =====================================================
    // FUNDING LEG
    // =====================================================
    private String fundingAmt;
    private String fundingCurrCd;
    private String fundingSetlAmt;
    private String fundingSetlCurrCd;
    private String fundingStatusReason;
    private String fundingNetworkCode;
    private String fundingTranProcessCd;
    private String fundingMsgTypeInd;
    private String fundingTransactionCategoryCode;
    private String fundingOrigNtwrkRespCd;
    private String fundingMerchantType;
    private String fundingIchgRateDsgnCd;
    private String fundingNetworkResponseCode;
    private String fundingNetworkResponseDesc;
    private String fundingSysTraceAudNum;
    private String fundingRetrievalRefNum;
    private String fundingSwitchSerialNumber;
    private String fundingUnqRefNum;
    private String fundingAuthIdResp;
    private String fundingNtwrkRefNum;
    private String fundingNetworkSendTimeStamp;
    private String fundingNetworkReceiveTimeStamp;
    private String fundingFundsAvailable;
    private String fundingQrData;

    // =====================================================
    // SETTLEMENT
    // =====================================================
    private String setlAmt;
    private String setlAmtExpnt;
    private String setlCurrCd;
    private String setlMMDT;
    private String setlServId;

    // =====================================================
    // SPONSOR BANK
    // =====================================================
    private String spnsrBankBusPartnerRefId;
    private String spnsrBankBusnPrtnrId;
    private String spnsrBankName;
    private String spnsrBankBusPrtnrId;
    private String sponsorBankPartnerIdId;
    private String sponsorBankId;
    private String sponsorBankName;

    // =====================================================
    // ORIGINATING INSTITUTION
    // =====================================================
    private String originatingInstitutionRefId;
    private String originatingInstitutionName;
    private String originatingInstName;
    private String originatingInstId;
    private String originatingApiName;
    private String originationCountry;

    // =====================================================
    // INSTITUTION
    // =====================================================
    private String institutionName;
    private String institutionCountry;

    // =====================================================
    // PARTNER
    // =====================================================
    private String partnerId;
    private String partnerNam;
    private String partnerRefId;
    private String partnerVer;

    // =====================================================
    // ERROR
    // =====================================================
    private String errorMessage;
    private String errorReasonCode;
    private String errorCodeDescription;
    private String errorCd;
    private String errorCdDesc;
    private String senderErrorReasonCode2;
    private String receiverErrorReasonCode2;

    // =====================================================
    // AVS / CVC
    // =====================================================
    private String cvcStatus;
    private String cvcResponseCode;
    private String cvcResponseDescription;
    private String cvcRespDesc;
    private String cvcStatCd;
    private String nameStatus;
    private String avsExtRefId;
    private String avsRespCd;
    private String avsRespDesc;

    // =====================================================
    // ADDRESS (AVS)
    // =====================================================
    private String frstNam;
    private String lstNam;
    private String stLn1Addr;
    private String stLn2Addr;
    private String cityNam;
    private String stPrvncCd;
    private String cntryCd;
    private String postCd;
    private String addrStatCd;
    private String postStatCd;

    // =====================================================
    // CLEARING
    // =====================================================
    private String clearingStatus;
    private String clearingDate;
    private String referenceId;
    private String acqRefNumber;
    private String reasonCode;
    private String reasonCodeDesc;
    private String interchangeRate;
    private String interchangeFee;
    private String systemTraceAuditNumber;
    private String corelationId;

    // =====================================================
    // MISC
    // =====================================================
    private String correlationId;
    private String traceId;
    private String createTimestamp;
    private String transmissionDateTime;
    private String processedDt;
    private String cutoffDt;
    private String brand;
    private String acceptanceBrand;
    private String brandProduct;
    private String productType;
    private String channel;
    private String deviceId;
    private String ecommerceInd;
    private String originalEcommerceIndicator;
    private String ucafDowngradeReason;
    private String msgTypeInd;
    private String singleDualMessageCd;
    private String singleDualMessageCode;
    private String posAuthDE022;
    private String posAuthDE061;
    private String pointOfServiceInteraction;
    private String switchSerialNumber;
    private String sysTraceAudNum;
    private String retrievalRefNum;
    private String unqRefNum;
    private String ichgFee;
    private String ichgRateDsgnCd;
    private String rateTypeIndicator;
    private String rateFileId;
    private String issuerCountryCd;
    private String issuerName;
    private String issuerIca;
    private String gatewayProcessor;
    private String paymentProcessor;
    private String paymentFacilitatorId;
    private String mdsProcessorId;
    private String processorId;
    private String participationId;
    private String profileId;
    private String serviceIndicator;
    private String initiationSource;
    private String statementDesc;
    private String tokenRequestorId;
    private String tokenRqstrId;
    private String qRData;
    private String onBehalfTranRef;
    private String onBhlfBusnPrtnrId;
    private String origDataElmtText;
    private String origNetworkDataText;
    private String originalNetworkData;
    private String originalRequestPayload;
    private String originalResponsePayload;
    private String bncRequestId;
    private String bncGWRequest;
    private String bncGWResponse;
    private String walletProviderSignature;
    private String openApiClientId;
    private String openApiRqstId;
    private String reversalId;
    private String reversalReference;
    private String reversalReason;
    private String revRsn;
    private String rfndId;
    private String rfndReason;
    private String rfndRef;
    private String reconExcpRsnId;
    private String autoRevSw;
    private String adviceRevRsnCd;
    private String crossborderTransactionIndicator;
    private String cutoffDateEstimateSw;
    private String dbWriteSuccessful;
    private String environmentCode;
    private String dsTransactionId;
    private String programProtocol;
    private String tokenCryptogramCryptoType;
    private String tokenCryptogramCryptoValue;
    private String tokenCryptogramPanSequenceNumber;
    private String authenticationValue;
    private String de126FraudScoreCode;
    private String cardNtwrkBinRngId;
    private String mastercardAssignedId;
    private String mappedCardExpirDt;
    private String mappedCardId;
    private String mappedVisaPanNum;
    private String subMerchantId;
    private String languageDataText;
    private String languageCode;
    private String addtlnProgDataTxt;
    private String availableBalanceAmount;
    private String rqstRefId;
    private String requestId;
    private String refId;
    private String fundVerTxt;
    private String fundingTranId2;
    private String enhancedResponse;
    private String paymentType;
    private String aisAccountType;
    private Long amount;
    private int nonFinTxn;

    // =====================================================
    // UK FPS
    // =====================================================
    private String ukfpsAddrUuid;
    private String ukfpsId;
    private String ukfpsPaymtUuid;
    private String ukfpsReceiverBic;
    private String ukfpsReceiverIban;
    private String ukfpsSenderBic;
    private String ukfpsSendIban;
    private String ukfpsSenderAcctName;
    private String ukfpsSenderSortCd;
    private String ukfpsReceiverAcctName;
    private String ukfpsReceiverSortCd;

    // =====================================================
    // DAILY LIMIT
    // =====================================================
    private String dailyLimitCurrNumTxt;
    private String dailyLimitCurrAmt;
    private String dailyLimitCurrExpNum;
    private String amountInUSD;

    // =====================================================
    // GATEWAY SLI
    // =====================================================
    private String origSliValCd;
    private String gatewaySliModRsnVal;
    private String gatewaySliValCd;
    private String comboCreditDebitIndicatorCode;

    // =====================================================
    // GETTERS AND SETTERS
    // =====================================================

    public String getAcctNum() { return acctNum; }
    public void setAcctNum(String acctNum) { this.acctNum = acctNum; }

    public String getAcctType() { return acctType; }
    public void setAcctType(String acctType) { this.acctType = acctType; }

    public String getAccountHolderName() { return accountHolderName; }
    public void setAccountHolderName(String accountHolderName) { this.accountHolderName = accountHolderName; }

    public String getAccountUri() { return accountUri; }
    public void setAccountUri(String accountUri) { this.accountUri = accountUri; }

    public String getAccountInformationId() { return accountInformationId; }
    public void setAccountInformationId(String accountInformationId) { this.accountInformationId = accountInformationId; }

    public String getAccountValidationId() { return accountValidationId; }
    public void setAccountValidationId(String accountValidationId) { this.accountValidationId = accountValidationId; }

    public String getSenderAccountNumberType() { return senderAccountNumberType; }
    public void setSenderAccountNumberType(String senderAccountNumberType) { this.senderAccountNumberType = senderAccountNumberType; }

    public String getReceiverAccountNumberType() { return receiverAccountNumberType; }
    public void setReceiverAccountNumberType(String receiverAccountNumberType) { this.receiverAccountNumberType = receiverAccountNumberType; }

    public String getPaymentAccountReference() { return paymentAccountReference; }
    public void setPaymentAccountReference(String paymentAccountReference) { this.paymentAccountReference = paymentAccountReference; }

    public String getAcqIca() { return acqIca; }
    public void setAcqIca(String acqIca) { this.acqIca = acqIca; }

    public String getAcquiringBin() { return acquiringBin; }
    public void setAcquiringBin(String acquiringBin) { this.acquiringBin = acquiringBin; }

    public String getAcquirerReferenceId() { return acquirerReferenceId; }
    public void setAcquirerReferenceId(String acquirerReferenceId) { this.acquirerReferenceId = acquirerReferenceId; }

    public String getAcqReferenceText() { return acqReferenceText; }
    public void setAcqReferenceText(String acqReferenceText) { this.acqReferenceText = acqReferenceText; }

    public String getAcqRefTxt() { return acqRefTxt; }
    public void setAcqRefTxt(String acqRefTxt) { this.acqRefTxt = acqRefTxt; }

    public String getAcquiringCountry() { return acquiringCountry; }
    public void setAcquiringCountry(String acquiringCountry) { this.acquiringCountry = acquiringCountry; }

    public String getAcquiringCountryCd() { return acquiringCountryCd; }
    public void setAcquiringCountryCd(String acquiringCountryCd) { this.acquiringCountryCd = acquiringCountryCd; }

    public String getAcquiringIdentificationCd() { return acquiringIdentificationCd; }
    public void setAcquiringIdentificationCd(String acquiringIdentificationCd) { this.acquiringIdentificationCd = acquiringIdentificationCd; }

    public String getAcquiringCredentialId() { return acquiringCredentialId; }
    public void setAcquiringCredentialId(String acquiringCredentialId) { this.acquiringCredentialId = acquiringCredentialId; }

    public String getAcquiringPrcssrId() { return acquiringPrcssrId; }
    public void setAcquiringPrcssrId(String acquiringPrcssrId) { this.acquiringPrcssrId = acquiringPrcssrId; }

    public String getAcquiringInstitutionId() { return acquiringInstitutionId; }
    public void setAcquiringInstitutionId(String acquiringInstitutionId) { this.acquiringInstitutionId = acquiringInstitutionId; }

    public String getVisaAcquiringBin() { return visaAcquiringBin; }
    public void setVisaAcquiringBin(String visaAcquiringBin) { this.visaAcquiringBin = visaAcquiringBin; }

    public String getTranId() { return tranId; }
    public void setTranId(String tranId) { this.tranId = tranId; }

    public String getTranAmt() { return tranAmt; }
    public void setTranAmt(String tranAmt) { this.tranAmt = tranAmt; }

    public String getTranAmtCurr() { return tranAmtCurr; }
    public void setTranAmtCurr(String tranAmtCurr) { this.tranAmtCurr = tranAmtCurr; }

    public String getTranAmtExpnt() { return tranAmtExpnt; }
    public void setTranAmtExpnt(String tranAmtExpnt) { this.tranAmtExpnt = tranAmtExpnt; }

    public String getTranAmtCurrNumeric() { return tranAmtCurrNumeric; }
    public void setTranAmtCurrNumeric(String tranAmtCurrNumeric) { this.tranAmtCurrNumeric = tranAmtCurrNumeric; }

    public String getTranTypeCd() { return tranTypeCd; }
    public void setTranTypeCd(String tranTypeCd) { this.tranTypeCd = tranTypeCd; }

    public String getTranTypeInd() { return tranTypeInd; }
    public void setTranTypeInd(String tranTypeInd) { this.tranTypeInd = tranTypeInd; }

    public String getTranProcessCd() { return tranProcessCd; }
    public void setTranProcessCd(String tranProcessCd) { this.tranProcessCd = tranProcessCd; }

    public String getTranOrigCountryCd() { return tranOrigCountryCd; }
    public void setTranOrigCountryCd(String tranOrigCountryCd) { this.tranOrigCountryCd = tranOrigCountryCd; }

    public String getTranOrigInstId() { return tranOrigInstId; }
    public void setTranOrigInstId(String tranOrigInstId) { this.tranOrigInstId = tranOrigInstId; }

    public String getTransactionCategoryCode() { return transactionCategoryCode; }
    public void setTransactionCategoryCode(String transactionCategoryCode) { this.transactionCategoryCode = transactionCategoryCode; }

    public String getTransactionLocalDateTime() { return transactionLocalDateTime; }
    public void setTransactionLocalDateTime(String transactionLocalDateTime) { this.transactionLocalDateTime = transactionLocalDateTime; }

    public String getTransactionLocalDateTimestamp() { return transactionLocalDateTimestamp; }
    public void setTransactionLocalDateTimestamp(String transactionLocalDateTimestamp) { this.transactionLocalDateTimestamp = transactionLocalDateTimestamp; }

    public String getTransactionOriginationIdentifier() { return transactionOriginationIdentifier; }
    public void setTransactionOriginationIdentifier(String transactionOriginationIdentifier) { this.transactionOriginationIdentifier = transactionOriginationIdentifier; }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getTransactionPurpose() { return transactionPurpose; }
    public void setTransactionPurpose(String transactionPurpose) { this.transactionPurpose = transactionPurpose; }

    public String getTransactionLinkId() { return transactionLinkId; }
    public void setTransactionLinkId(String transactionLinkId) { this.transactionLinkId = transactionLinkId; }

    public String getTranClearDtlId() { return tranClearDtlId; }
    public void setTranClearDtlId(String tranClearDtlId) { this.tranClearDtlId = tranClearDtlId; }

    public String getNetworkCode() { return networkCode; }
    public void setNetworkCode(String networkCode) { this.networkCode = networkCode; }

    public String getNetworkGateway() { return networkGateway; }
    public void setNetworkGateway(String networkGateway) { this.networkGateway = networkGateway; }

    public String getOrigNetworkGateway() { return origNetworkGateway; }
    public void setOrigNetworkGateway(String origNetworkGateway) { this.origNetworkGateway = origNetworkGateway; }

    public String getNetworkReceiveTimeStamp() { return networkReceiveTimeStamp; }
    public void setNetworkReceiveTimeStamp(String networkReceiveTimeStamp) { this.networkReceiveTimeStamp = networkReceiveTimeStamp; }

    public String getNetworkResponseCode() { return networkResponseCode; }
    public void setNetworkResponseCode(String networkResponseCode) { this.networkResponseCode = networkResponseCode; }

    public String getNetworkResponseCodeDesc() { return networkResponseCodeDesc; }
    public void setNetworkResponseCodeDesc(String networkResponseCodeDesc) { this.networkResponseCodeDesc = networkResponseCodeDesc; }

    public String getNetworkSendTimeStamp() { return networkSendTimeStamp; }
    public void setNetworkSendTimeStamp(String networkSendTimeStamp) { this.networkSendTimeStamp = networkSendTimeStamp; }

    public String getNtwrkRefNum() { return ntwrkRefNum; }
    public void setNtwrkRefNum(String ntwrkRefNum) { this.ntwrkRefNum = ntwrkRefNum; }

    public String getNetworkReferenceNum() { return networkReferenceNum; }
    public void setNetworkReferenceNum(String networkReferenceNum) { this.networkReferenceNum = networkReferenceNum; }

    public String getNtwrkStatDesc() { return ntwrkStatDesc; }
    public void setNtwrkStatDesc(String ntwrkStatDesc) { this.ntwrkStatDesc = ntwrkStatDesc; }

    public String getProcessedNetwork() { return processedNetwork; }
    public void setProcessedNetwork(String processedNetwork) { this.processedNetwork = processedNetwork; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getStatusReason() { return statusReason; }
    public void setStatusReason(String statusReason) { this.statusReason = statusReason; }

    public String getStatusTimestamp() { return statusTimestamp; }
    public void setStatusTimestamp(String statusTimestamp) { this.statusTimestamp = statusTimestamp; }

    public String getInternalStatus() { return internalStatus; }
    public void setInternalStatus(String internalStatus) { this.internalStatus = internalStatus; }

    public String getFundsAvailable() { return fundsAvailable; }
    public void setFundsAvailable(String fundsAvailable) { this.fundsAvailable = fundsAvailable; }

    public String getFundAvailable() { return fundAvailable; }
    public void setFundAvailable(String fundAvailable) { this.fundAvailable = fundAvailable; }

    public String getFuncCode() { return funcCode; }
    public void setFuncCode(String funcCode) { this.funcCode = funcCode; }

    public String getAuthIdResp() { return authIdResp; }
    public void setAuthIdResp(String authIdResp) { this.authIdResp = authIdResp; }

    public String getOrigNtwrkRespCd() { return origNtwrkRespCd; }
    public void setOrigNtwrkRespCd(String origNtwrkRespCd) { this.origNtwrkRespCd = origNtwrkRespCd; }

    public String getNetworkDecisionCode() { return networkDecisionCode; }
    public void setNetworkDecisionCode(String networkDecisionCode) { this.networkDecisionCode = networkDecisionCode; }

    public String getSndrAcct() { return sndrAcct; }
    public void setSndrAcct(String sndrAcct) { this.sndrAcct = sndrAcct; }

    public String getSndrAcctUri() { return sndrAcctUri; }
    public void setSndrAcctUri(String sndrAcctUri) { this.sndrAcctUri = sndrAcctUri; }

    public String getSndrCardExpirDt() { return sndrCardExpirDt; }
    public void setSndrCardExpirDt(String sndrCardExpirDt) { this.sndrCardExpirDt = sndrCardExpirDt; }

    public String getSndrAddrLine1() { return sndrAddrLine1; }
    public void setSndrAddrLine1(String sndrAddrLine1) { this.sndrAddrLine1 = sndrAddrLine1; }

    public String getSndrAddrLine2() { return sndrAddrLine2; }
    public void setSndrAddrLine2(String sndrAddrLine2) { this.sndrAddrLine2 = sndrAddrLine2; }

    public String getSndrBirthCountry() { return sndrBirthCountry; }
    public void setSndrBirthCountry(String sndrBirthCountry) { this.sndrBirthCountry = sndrBirthCountry; }

    public String getSndrBirthDt() { return sndrBirthDt; }
    public void setSndrBirthDt(String sndrBirthDt) { this.sndrBirthDt = sndrBirthDt; }

    public String getSndrCityName() { return sndrCityName; }
    public void setSndrCityName(String sndrCityName) { this.sndrCityName = sndrCityName; }

    public String getSndrCountryCd() { return sndrCountryCd; }
    public void setSndrCountryCd(String sndrCountryCd) { this.sndrCountryCd = sndrCountryCd; }

    public String getSndrCountrySubCd() { return sndrCountrySubCd; }
    public void setSndrCountrySubCd(String sndrCountrySubCd) { this.sndrCountrySubCd = sndrCountrySubCd; }

    public String getSndrDigitalAcctRefNum() { return sndrDigitalAcctRefNum; }
    public void setSndrDigitalAcctRefNum(String sndrDigitalAcctRefNum) { this.sndrDigitalAcctRefNum = sndrDigitalAcctRefNum; }

    public String getSndrEmailAddr() { return sndrEmailAddr; }
    public void setSndrEmailAddr(String sndrEmailAddr) { this.sndrEmailAddr = sndrEmailAddr; }

    public String getSndrFirstName() { return sndrFirstName; }
    public void setSndrFirstName(String sndrFirstName) { this.sndrFirstName = sndrFirstName; }

    public String getSndrLastName() { return sndrLastName; }
    public void setSndrLastName(String sndrLastName) { this.sndrLastName = sndrLastName; }

    public String getSndrMiddleName() { return sndrMiddleName; }
    public void setSndrMiddleName(String sndrMiddleName) { this.sndrMiddleName = sndrMiddleName; }

    public String getSndrNationalityCd() { return sndrNationalityCd; }
    public void setSndrNationalityCd(String sndrNationalityCd) { this.sndrNationalityCd = sndrNationalityCd; }

    public String getSndrPaymentFacilitatorId() { return sndrPaymentFacilitatorId; }
    public void setSndrPaymentFacilitatorId(String sndrPaymentFacilitatorId) { this.sndrPaymentFacilitatorId = sndrPaymentFacilitatorId; }

    public String getSndrSubMerchantId() { return sndrSubMerchantId; }
    public void setSndrSubMerchantId(String sndrSubMerchantId) { this.sndrSubMerchantId = sndrSubMerchantId; }

    public String getSndrPhoneNum() { return sndrPhoneNum; }
    public void setSndrPhoneNum(String sndrPhoneNum) { this.sndrPhoneNum = sndrPhoneNum; }

    public String getSndrPostalCd() { return sndrPostalCd; }
    public void setSndrPostalCd(String sndrPostalCd) { this.sndrPostalCd = sndrPostalCd; }

    public String getSndrSanctionScore() { return sndrSanctionScore; }
    public void setSndrSanctionScore(String sndrSanctionScore) { this.sndrSanctionScore = sndrSanctionScore; }

    public String getSndrFromAccount() { return sndrFromAccount; }
    public void setSndrFromAccount(String sndrFromAccount) { this.sndrFromAccount = sndrFromAccount; }

    public String getSndrOrigName() { return sndrOrigName; }
    public void setSndrOrigName(String sndrOrigName) { this.sndrOrigName = sndrOrigName; }

    public String getSenderAliasTypeCd() { return senderAliasTypeCd; }
    public void setSenderAliasTypeCd(String senderAliasTypeCd) { this.senderAliasTypeCd = senderAliasTypeCd; }

    public String getSenderAliasValTxt() { return senderAliasValTxt; }
    public void setSenderAliasValTxt(String senderAliasValTxt) { this.senderAliasValTxt = senderAliasValTxt; }

    public String getSenderErrorReasonCode() { return senderErrorReasonCode; }
    public void setSenderErrorReasonCode(String senderErrorReasonCode) { this.senderErrorReasonCode = senderErrorReasonCode; }

    public String getSenderErrorReasonCodeDescription() { return senderErrorReasonCodeDescription; }
    public void setSenderErrorReasonCodeDescription(String v) { this.senderErrorReasonCodeDescription = v; }

    public String getSenderResponseReasonCode() { return senderResponseReasonCode; }
    public void setSenderResponseReasonCode(String senderResponseReasonCode) { this.senderResponseReasonCode = senderResponseReasonCode; }

    public String getSenderResponseReasonDetail() { return senderResponseReasonDetail; }
    public void setSenderResponseReasonDetail(String senderResponseReasonDetail) { this.senderResponseReasonDetail = senderResponseReasonDetail; }

    public int getSenderEligible() { return senderEligible; }
    public void setSenderEligible(int senderEligible) { this.senderEligible = senderEligible; }

    public List<GovtId> getSndrGovtId() { return sndrGovtId; }
    public void setSndrGovtId(List<GovtId> sndrGovtId) { this.sndrGovtId = sndrGovtId; }

    public String getRcvrAcct() { return rcvrAcct; }
    public void setRcvrAcct(String rcvrAcct) { this.rcvrAcct = rcvrAcct; }

    public String getRcvrAcctUri() { return rcvrAcctUri; }
    public void setRcvrAcctUri(String rcvrAcctUri) { this.rcvrAcctUri = rcvrAcctUri; }

    public String getRcvrAddrLine1() { return rcvrAddrLine1; }
    public void setRcvrAddrLine1(String rcvrAddrLine1) { this.rcvrAddrLine1 = rcvrAddrLine1; }

    public String getRcvrAddrLine2() { return rcvrAddrLine2; }
    public void setRcvrAddrLine2(String rcvrAddrLine2) { this.rcvrAddrLine2 = rcvrAddrLine2; }

    public String getRcvrBirthCountry() { return rcvrBirthCountry; }
    public void setRcvrBirthCountry(String rcvrBirthCountry) { this.rcvrBirthCountry = rcvrBirthCountry; }

    public String getRcvrBirthDate() { return rcvrBirthDate; }
    public void setRcvrBirthDate(String rcvrBirthDate) { this.rcvrBirthDate = rcvrBirthDate; }

    public String getRcvrCardExpirDt() { return rcvrCardExpirDt; }
    public void setRcvrCardExpirDt(String rcvrCardExpirDt) { this.rcvrCardExpirDt = rcvrCardExpirDt; }

    public String getRcvrCity() { return rcvrCity; }
    public void setRcvrCity(String rcvrCity) { this.rcvrCity = rcvrCity; }

    public String getRcvrCountryCd() { return rcvrCountryCd; }
    public void setRcvrCountryCd(String rcvrCountryCd) { this.rcvrCountryCd = rcvrCountryCd; }

    public String getRcvrCountrySubCd() { return rcvrCountrySubCd; }
    public void setRcvrCountrySubCd(String rcvrCountrySubCd) { this.rcvrCountrySubCd = rcvrCountrySubCd; }

    public String getRcvrEmailAddr() { return rcvrEmailAddr; }
    public void setRcvrEmailAddr(String rcvrEmailAddr) { this.rcvrEmailAddr = rcvrEmailAddr; }

    public String getRcvrExternalAcctRefId() { return rcvrExternalAcctRefId; }
    public void setRcvrExternalAcctRefId(String rcvrExternalAcctRefId) { this.rcvrExternalAcctRefId = rcvrExternalAcctRefId; }

    public String getRcvrExternalRefId() { return rcvrExternalRefId; }
    public void setRcvrExternalRefId(String rcvrExternalRefId) { this.rcvrExternalRefId = rcvrExternalRefId; }

    public String getRcvrFirstName() { return rcvrFirstName; }
    public void setRcvrFirstName(String rcvrFirstName) { this.rcvrFirstName = rcvrFirstName; }

    public String getRcvrLastName() { return rcvrLastName; }
    public void setRcvrLastName(String rcvrLastName) { this.rcvrLastName = rcvrLastName; }

    public String getRcvrMiddleName() { return rcvrMiddleName; }
    public void setRcvrMiddleName(String rcvrMiddleName) { this.rcvrMiddleName = rcvrMiddleName; }

    public String getRcvrNationalityCd() { return rcvrNationalityCd; }
    public void setRcvrNationalityCd(String rcvrNationalityCd) { this.rcvrNationalityCd = rcvrNationalityCd; }

    public String getRcvrPaymentFacilitatorId() { return rcvrPaymentFacilitatorId; }
    public void setRcvrPaymentFacilitatorId(String rcvrPaymentFacilitatorId) { this.rcvrPaymentFacilitatorId = rcvrPaymentFacilitatorId; }

    public String getRcvrPhoneNum() { return rcvrPhoneNum; }
    public void setRcvrPhoneNum(String rcvrPhoneNum) { this.rcvrPhoneNum = rcvrPhoneNum; }

    public String getRcvrPostalCd() { return rcvrPostalCd; }
    public void setRcvrPostalCd(String rcvrPostalCd) { this.rcvrPostalCd = rcvrPostalCd; }

    public String getRcvrSanctionScore() { return rcvrSanctionScore; }
    public void setRcvrSanctionScore(String rcvrSanctionScore) { this.rcvrSanctionScore = rcvrSanctionScore; }

    public String getRcvrSubMerchantId() { return rcvrSubMerchantId; }
    public void setRcvrSubMerchantId(String rcvrSubMerchantId) { this.rcvrSubMerchantId = rcvrSubMerchantId; }

    public String getRcvrTokenValue() { return rcvrTokenValue; }
    public void setRcvrTokenValue(String rcvrTokenValue) { this.rcvrTokenValue = rcvrTokenValue; }

    public String getRcvrOrigName() { return rcvrOrigName; }
    public void setRcvrOrigName(String rcvrOrigName) { this.rcvrOrigName = rcvrOrigName; }

    public String getRcvrAliasTypeCd() { return rcvrAliasTypeCd; }
    public void setRcvrAliasTypeCd(String rcvrAliasTypeCd) { this.rcvrAliasTypeCd = rcvrAliasTypeCd; }

    public String getRcvrAliasValue() { return rcvrAliasValue; }
    public void setRcvrAliasValue(String rcvrAliasValue) { this.rcvrAliasValue = rcvrAliasValue; }

    public String getReceiverErrorReasonCode() { return receiverErrorReasonCode; }
    public void setReceiverErrorReasonCode(String receiverErrorReasonCode) { this.receiverErrorReasonCode = receiverErrorReasonCode; }

    public String getReceiverErrorReasonCodeDescription() { return receiverErrorReasonCodeDescription; }
    public void setReceiverErrorReasonCodeDescription(String v) { this.receiverErrorReasonCodeDescription = v; }

    public String getReceiverResponseReasonCode() { return receiverResponseReasonCode; }
    public void setReceiverResponseReasonCode(String receiverResponseReasonCode) { this.receiverResponseReasonCode = receiverResponseReasonCode; }

    public String getReceiverResponseReasonDetail() { return receiverResponseReasonDetail; }
    public void setReceiverResponseReasonDetail(String receiverResponseReasonDetail) { this.receiverResponseReasonDetail = receiverResponseReasonDetail; }

    public int getReceiverEligible() { return receiverEligible; }
    public void setReceiverEligible(int receiverEligible) { this.receiverEligible = receiverEligible; }

    public List<GovtId> getRcvrGovtId() { return rcvrGovtId; }
    public void setRcvrGovtId(List<GovtId> rcvrGovtId) { this.rcvrGovtId = rcvrGovtId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public String getPartnerId() { return partnerId; }
    public void setPartnerId(String partnerId) { this.partnerId = partnerId; }

    public String getPartnerNam() { return partnerNam; }
    public void setPartnerNam(String partnerNam) { this.partnerNam = partnerNam; }

    public String getInstitutionName() { return institutionName; }
    public void setInstitutionName(String institutionName) { this.institutionName = institutionName; }

    public String getInstitutionCountry() { return institutionCountry; }
    public void setInstitutionCountry(String institutionCountry) { this.institutionCountry = institutionCountry; }

    public String getBrand() { return brand; }
    public void setBrand(String brand) { this.brand = brand; }

    public String getAcceptanceBrand() { return acceptanceBrand; }
    public void setAcceptanceBrand(String acceptanceBrand) { this.acceptanceBrand = acceptanceBrand; }

    public String getClearingStatus() { return clearingStatus; }
    public void setClearingStatus(String clearingStatus) { this.clearingStatus = clearingStatus; }

    public String getClearingDate() { return clearingDate; }
    public void setClearingDate(String clearingDate) { this.clearingDate = clearingDate; }

    public String getReferenceId() { return referenceId; }
    public void setReferenceId(String referenceId) { this.referenceId = referenceId; }

    public String getSwitchSerialNumber() { return switchSerialNumber; }
    public void setSwitchSerialNumber(String switchSerialNumber) { this.switchSerialNumber = switchSerialNumber; }

    public String getOriginalRequestPayload() { return originalRequestPayload; }
    public void setOriginalRequestPayload(String originalRequestPayload) { this.originalRequestPayload = originalRequestPayload; }

    public String getOriginalResponsePayload() { return originalResponsePayload; }
    public void setOriginalResponsePayload(String originalResponsePayload) { this.originalResponsePayload = originalResponsePayload; }

    public int getNonFinTxn() { return nonFinTxn; }
    public void setNonFinTxn(int nonFinTxn) { this.nonFinTxn = nonFinTxn; }

    public Long getAmount() { return amount; }
    public void setAmount(Long amount) { this.amount = amount; }
}