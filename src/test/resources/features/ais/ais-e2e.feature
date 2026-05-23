Feature: AIS Event End-to-End Test

Background:
    * url baseUrl

Scenario: AIS event should be processed and stored in DB

    # Read request JSON
    * def requestPayload = read('ais-payload.json')

    # Parse nested payload
    * def payload = karate.fromString(requestPayload.eventPayload)

    # API CALL
    Given path '/ingestion'
    And request requestPayload
    When method POST
    Then status 202

    # RESPONSE
    * def apiResponse = response

    * print 'API RESPONSE:', apiResponse

    * match apiResponse.status == 'SUCCESS'
    * match apiResponse.eventId == requestPayload.eventId

    # =====================================================
    # SEND_TRANSACTIONS
    # =====================================================

    * def transaction = db.getTransaction(payload.accountInformationId)

    * print 'SEND_TRANSACTIONS:', transaction

    * match transaction.TRAN_ID == payload.accountInformationId
    * match transaction.TRAN_TYPE == payload.transactionType
    * match transaction.SWITCH_STAT == payload.switchStatus
    * match transaction.SWITCH_STAT_DESC == payload.switchStatusDesc
    * match transaction.REF_ID == payload.referenceId
    * match transaction.TRANFR_ACPT_ID == payload.partnerId
    * match transaction.TRANFR_ACPT_NAM contains payload.partnerName
    * match transaction.ORIG_INST_NAM contains payload.partnerName
    * match transaction.ACCT_NUM == payload.accountUri
    * match transaction.ACCT_TYPE == payload.accountType
    * match transaction.TRAN_CURR == payload.accountStatementCurrency
    * match transaction.TRAN_AMT == payload.transactionAmount.toString()
    * match transaction.NTWRK_CD == payload.network
    * match transaction.NTWRK_RESP_CD == payload.brand
    * match transaction.NTWRK_RESP_CD_DESC == payload.acceptanceBrand
    * match transaction.USE_CASE == payload.paymentType
    * match transaction.SW_SER_NUM == payload.switchSerialNumber

    # =====================================================
    # SEND_TRAN_DTL
    # =====================================================

    * def tranDtl = db.getTransactionDetails(payload.accountInformationId)

    * print 'SEND_TRAN_DTL:', tranDtl

    * match tranDtl.PAYMT_REF == payload.referenceId
    * match tranDtl.ACQ_CNTRY_NAM == payload.country
    * match tranDtl.ACQ_ICA == payload.brand
    * match tranDtl.FUND_SRC == payload.partnerName
    * match tranDtl.PAYMT_TYPE == payload.paymentType
    * match tranDtl.POINT_SERV_INTRCTN == payload.network
    * match tranDtl.TRAN_PRPS == payload.paymentType
    * match tranDtl.TRAN_SETL_AMT == payload.transactionAmount.toString()
    * match tranDtl.ORIG_RQST_PYLD == payload.originalRequestPayload
    * match tranDtl.ORIG_RESP_PYLD == payload.originalResponsePayload
    * match tranDtl.PROC_ID == payload.partnerId
    * match tranDtl.ACQ_IDEN_CD == payload.acceptanceBrand

    # =====================================================
    # SEND_RECIP_DTL
    # =====================================================

    * def recipDtl = db.getRecipientDetails(payload.accountInformationId)

    * print 'SEND_RECIP_DTL:', recipDtl

    * match recipDtl.SEND_ACCT_NUM == payload.accountUri
    * match recipDtl.SEND_ACCT_NUM_TYPE == payload.accountType
    * match recipDtl.RECIP_ACCT_NUM == payload.accountUri
    * match recipDtl.RECIP_ACCT_NUM_TYPE == payload.accountType
    * match recipDtl.SEND_ACCT_URI == payload.accountUri
    * match recipDtl.RECIP_ACCT_URI == payload.accountUri
    * match recipDtl.SEND_CNTRY_NAM == payload.country
    * match recipDtl.RECIP_CNTRY_NAM == payload.country

    # =====================================================
    # SEND_TRAN_ADDR_DTL
    # =====================================================

    * def addrDtl = db.getAddressDetails(payload.accountInformationId)

    * print 'SEND_TRAN_ADDR_DTL:', addrDtl