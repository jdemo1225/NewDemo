package com.alchemain.rx.utils;

public interface Constants {
	// internal
	public static final String GLOBAL = "global";
	public static final boolean DEBUG = false;
	public static final String _ID = "_id";
	public static final String PROCESS_ID = "processId";
	public static final String DATA = "data";
	public static final String DELEGATE = "delegate";
	public static final String JOB_ID = "jobId";
	public static final String ITERATIONS = "iterations";
	public static final String REMAINING = "remaining";
	public static final String RESULT = "result";
	public static final String DATA_ITERATIONS_REMAINING = String.format("%s.%s.%s", DATA, ITERATIONS, REMAINING);
	public static final String DATA_ITERATIONS_RESULT = String.format("%s.%s.%s", DATA, ITERATIONS, RESULT);
	public static final String JOIN_ON = "joinOn";
	public static final String CREATE_TIME = "createTime";
	public static final String FAILED_STATE_ID = "failedStateid";
	public static final String ERROR = "error";
	public static final String CONTEXT = "context";
	public static final String ENDPOINT = "endpoint";
	public static final String TENANT = "tenant";
	public static final String SYSTEM = "system";

	/* CONNECTOR TYPES */
	public static enum ConnectorType {
		CLP, ICE, IC2;
	}

	/* ERROR CODES */
	public static enum ErrorCode {
		CLP_TICKET_MISSING_ACCOUNT_ID;
	}

	/* HTTP Constants */
	public static final String AUTHORIZATION = "authorization";
	public static final String ACCEPT = "accept";
	public static final String CONTENT_TYPE = "content-type";

	// HTTP timeout (5 minutes)
	static final int TIMEOUT = 1000 * 60 * 5;

	/* SCUID headers */
	public static final String X_PROMISE = "x-scuid-promise";
	public static final String X_TENANT = "x-scuid-tenant";
	public static final String X_USER = "x-scuid-user";

	/* Multi-valued attributes */
	public static final String PRIMARY = "primary";
	public static final String VALUE = "value";

	/* Email variables */
	public static final String TO_PERSON = "toPerson";
	public static final String FOR_PERSON = "forPerson";
	public static final String FOR_ENTITLEMENT = "forEntitlement";
	public static final String FOR_REQUEST_UNIT = "forRequestUnit";
	public static final String EMAIL_TYPE = "emailType";
	public static final String SUBJECT = "subject";
	public static final String BODY = "body";
	public static final String HTML_BODY = "htmlbody";
	public static final String EMAIL = "email"; 

	/* Task Decisions */
	public static final String APPROVED = "APPROVED";
	public static final String DENIED = "DENIED";

	/* Process states */
	public static final String COMPLETE = "COMPLETE";
	public static final String FAILED = "FAILED";

	/* Person/Account states */
	public static final String DELETED = "DELETED";
	public static final String TERMINATED = "TERMINATED";
	public static final String DISABLED = "DISABLED";

	/* Cache operations */
	public static final String OPERATION = "operation";
	public static final String DELETE = "delete";
	public static final String ADD = "add";

	/* Process variables */
	public static final String SELF = "self";
	public static final String ACCOUNTS = "accounts";
	public static final String ACCOUNT = "account";
	public static final String ACCOUNT_ID = "accountId";
	public static final String ADMIN_NOTIFIED = "adminNotified";
	public static final String APPROVAL_TASK_DECISION = "approvalTaskDecision";
	public static final String APPROVAL_WORKFLOW = "approvalWorkflow";
	public static final String ASSIGNED_TO = "assignedTo";
	public static final String ASSIGNEE = "assignee";
	public static final String ATTRIBUTES = "attributes";
	public static final String AUTO_APPROVE = "autoApprove";
	public static final String BENEFICIARY = "beneficiary";
	public static final String BIRTHRIGHT_ENTITLEMENTS = "birthRightEntitlements";
	public static final String BIRTHRIGHT_ENTITLEMENTS_FOUND = "birthRightEntitlementsFound";
	public static final String CHANNEL = "channel";
	public static final String COMMENT = "comment";
	public static final String CONFIG_LOCATION = "configLocation";
	public static final String CONNECTOR_RESPONSE = "connectorResponse";
	public static final String CONNECTOR_TYPE = "connectorType";
	public static final String TICKET_STATUS = "ticketStatus";
	public static final String CREATED_TRANS_ID = "_createdTransId";
	public static final String DESCRIPTION = "description";
	public static final String EMAILS = "emails";
	public static final String ENTITLEMENT = "entitlement";
	public static final String ENTITLEMENT_ID = "entitlementId";
	public static final String ENTITLEMENTS = "entitlements";
	public static final String FULFILLMENT_WORKFLOW = "fulfillmentWorkflow";
	public static final String GROUPS = "groups";
	public static final String GROUP = "group";
	public static final String ID = "id";
	public static final String NAME = "name";
	public static final String DISPLAY_NAME = "displayName";
	public static final String INSTRUCTIONS = "instructions";
	public static final String INSTRUCTIONS_TICKET_ID = "instructions.ticket.ticketId";
	public static final String INSTRUCTIONS_TICKET_STATUS = "instructions.ticket.ticketStatus";
	public static final String MANAGER = "manager";
	public static final String MANAGER_ID = "managerId";
	public static final String METHOD = "method";
	public static final String MINUTES = "minutes";
	public static final String NEW_ACCOUNT_BENEFICIARY_ID = "newAccountBeneficiaryId";
	public static final String NEW_ACCOUNT_ID = "newAccountId";
	public static final String NODE_TYPE = "_nodeType";
	public static final String NOTIFICATION_DATA = "notificationData";
	public static final String NOTIFICATION_TYPE = "notificationType";
	public static final String PAYLOAD = "payload";
	public static final String PASSWORD = "password";
	public static final String PERSON = "person";
	public static final String PERSON_ID = "personId";
	public static final String PERSON_DATA = "personData";
	public static final String PROVISIONED_ENTITLEMENT = "provisionedEntitlement";
	public static final String PROVISION_STATUS = "provisionStatus";
	public static final String QUERY = "query";
	public static final String RECIPIENT = "recipient";
	public static final String REPLACEMENT = "replacement";
	public static final String REQUESTED_BY = "requestedBy";
	public static final String REQUESTED_FOR = "requestedFor";
	public static final String REQUEST_ID = "requestId";
	public static final String REQUEST_TYPE = "requestType";
	public static final String REQUESTUNIT = "requestUnit";
	public static final String REQUESTUNITS = "requestUnits";
	public static final String REQUESTUNIT_ID = "requestUnitId";
	public static final String REQUESTUNIT_IDS = "requestUnitIds";
	public static final String RESOURCE_ID = "resourceId";
	public static final String RESOURCE_TYPE = "resourceType";
	public static final String SCUID_STATUS = "scuidStatus";
	public static final String SOURCE = "source";
	public static final String STATUS = "status";
	public static final String SUBJECTS = "subjects";
	public static final String RAW = "raw";
	public static final String RESPONSE = "response";
	public static final String SYNCHRONIZE_MASTER_PASSWORD = "synchronizeMasterPassword";
	public static final String TARGET_SYSTEM = "targetSystem";
	public static final String TERMINATED_BY = "terminatedBy";
	public static final String TERMINATED_ON = "terminatedOn";
	public static final String TERMINATED_USER = "terminatedUser";
	public static final String TERMINATION_REASON = "terminationReason";
	public static final String LAST_MODIFIED_BY = "_lastModifiedBy";
	public static final String UPDATED_ON = "updatedOn";
	public static final String UPDATE_REASON = "updateReason";
	public static final String TYPE = "type";
	public static final String BIRTHRIGHT_GRANT = "BIRTHRIGHT_GRANT";
	public static final String UPDATE = "update";
	public static final String UPDATED_COUNT = "updatedCount";
	public static final String USERNAME = "userName";
	public static final String VALID = "valid";
	public static final String RESOURCE = "resource";
	public static final String ACCOUNT_RETENTION_PERIOD = "accountRetentionPeriod";
	public static final String DELETE_ACCOUNT_ON = "deleteAccountOn";
	public static final String ACTIVE = "active";
	public static final String ACCOUNTS_TO_DEPROVISION = "accountsToDeprovision";
	public static final String ATTRIBUTE_MAP = "attributeMap";
	public static final String ACCOUNT_TYPE = "accountType";
	public static final String TICKET = "ticket";
	public static final String TICKETS = "tickets";
	public static final String TICKET_ID = "ticketId";
	public static final String PROVISIONING_POLICY_IDS = "provisioningPolicyIds";
	public static final String BIRTHRIGHT_PROVISIONED_BY = "birthrightProvisionedBy";
	public static final String SCHEDULE = "schedule";
	public static final String APPLICATION_ID = "applicationId";
	public static final String APPLICATION = "application";
	public static final String STARTED_BY = "startedBy";
	public static final String GIVEN_NAME = "givenName";
	public static final String FAMILY_NAME = "familyName";
	public static final String OWNER = "owner";
	public static final String TOPIC = "topic";
	public static final String REPORTS = "reports";
	public static final String SYNC_STATUS = "syncStatus";
	public static final String WORK_EMAIL = "workEmail";
	public static final String PERSONAL_EMAIL = "personalEmail";
	public static final String CREATED_USING = "createdUsing";
	public static final String ORGUNIT = "orgunit";
	public static final String ORGUNITS = "orgunits";
	public static final String CREATED_FROM = "createdFrom";
	
	/* Mongo collection key identifiers */
	public static final String SYSTEM_CODE = "systemCode";
	public static final String USERNAME_GENERATION_POLICY = "USERNAME_GENERATION_POLICY";

	/* Task states */
	public static final String CANCELLED = "CANCELLED";
	public static final String PENDING = "PENDING";
	public static final String PENDING_APPROVAL = "PENDING_APPROVAL";
	public static final String PROCESSING = "PROCESSING";
	public static final String EXECUTING = "EXECUTING";
	public static final String PROCESS_DEFINITION_KEY = "processDefinitionKey";
	public static final String NO_OP = "NoOp";

	/* Task types */
	public static final String APPROVAL = "APPROVAL";

	public static final String DATE_ISO_8601 = "yyyy-MM-dd'T'hh:mm:ss.SSSZ";

	/* SCIM Constants */
	public static final String SCHEMAS = "schemas";
	public static final String CORE_SCHEMA = "urn:scim:schemas:core:1.0";
	public static final String EXTENSION_SCHEMA = "urn:scim:schemas:extension:scuid:1.0";
	
	/* Elasticsearch Constants */
	public static final Integer DEFAULT_FROM = 0;
	public static final Integer DEFAULT_SIZE = 20;
	public static final String HITS = "hits";
	public static final String TOTAL = "total";
	public static final String _SOURCE = "_source";

	/* Paged Response Constants */
	public static final String PAGING = "paging";
	public static final String LINKS = "links";
	public static final String FIRST = "first";
	public static final String PREVIOUS = "previous";
	public static final String NEXT = "next";
	public static final String LAST = "last";
	public static final String COUNT = "count";
	public static final String TOTAL_COUNT = "totalCount";
	public static final String OFFSET = "offset";
	public static final String LIMIT = "limit";
	
	/* collection names */
	public static final String JOBS = "jobs";
	public static final String RULES = "RULES";
	public static final String SYNCERS = "SYNCERS";
}
