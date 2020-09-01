import { r } from "../../models";
import { getConfig, hasConfig } from "../../api/lib/config";
import { symmetricDecrypt } from "../../api/lib/crypto";

const cacheKey = orgId => `${process.env.CACHE_PREFIX || ""}org-${orgId}`;

const organizationCache = {
  clear: async id => {
    if (r.redis) {
      await r.redis.delAsync(cacheKey(id));
    }
  },
  getMessageServiceSid: async (organization, contact, message) => {
    // Note organization won't always be available, so we'll need to conditionally look it up based on contact
    if (message) {
      if (message.text && /twilioapitest/.test(message.text)) {
        return "fakeSid_MK123";
      }
      const globalMessageServiceSid = getConfig(
        "TWILIO_MESSAGE_SERVICE_SID",
        organization
      );
      return globalMessageServiceSid === message.messageservice_sid
        ? globalMessageServiceSid
        : message.messageservice_sid;
    } else {
      return getConfig("TWILIO_MESSAGE_SERVICE_SID", organization);
    }
  },
  getTwilioAuth: async organization => {
    const hasOrgToken = hasConfig("TWILIO_AUTH_TOKEN_ENCRYPTED", organization);
    // Note, allows unencrypted auth tokens to be (manually) stored in the db
    // @todo: decide if this is necessary, or if UI/envars is sufficient.
    const authToken = hasOrgToken
      ? symmetricDecrypt(getConfig("TWILIO_AUTH_TOKEN_ENCRYPTED", organization))
      : getConfig("TWILIO_AUTH_TOKEN", organization);
    const accountSid = hasConfig("TWILIO_ACCOUNT_SID", organization)
      ? getConfig("TWILIO_ACCOUNT_SID", organization)
      : // Check old TWILIO_API_KEY variable for backwards compatibility.
        getConfig("TWILIO_API_KEY", organization);
    return { authToken, accountSid };
  },
  load: async id => {
    if (r.redis) {
      const orgData = await r.redis.getAsync(cacheKey(id));
      if (orgData) {
        return JSON.parse(orgData);
      }
    }
    const [dbResult] = await r
      .knex("organization")
      .where("id", id)
      .select("*")
      .limit(1);
    if (dbResult) {
      if (dbResult.features) {
        dbResult.feature = JSON.parse(dbResult.features);
      } else {
        dbResult.feature = {};
      }
      if (r.redis) {
        await r.redis
          .multi()
          .set(cacheKey(id), JSON.stringify(dbResult))
          .expire(cacheKey(id), 43200)
          .execAsync();
      }
    }
    return dbResult;
  },
  load_from_messageservice: async messageservice_sid => {
    if (r.redis) {
      const orgData = await r.redis.getAsync(cacheKey(messageservice_sid));
      if (orgData) {
        return JSON.parse(orgData);
      }
    }
    const dbResult = await r.knex.raw(
      `SELECT * FROM organization WHERE features LIKE '%${messageservice_sid}%';`
    );
    if (dbResult) {
      if (dbResult.features) {
        dbResult.feature = JSON.parse(dbResult.features);
      } else {
        dbResult.feature = {};
      }
      if (r.redis) {
        await r.redis
          .multi()
          .set(cacheKey(messageservice_sid), JSON.stringify(dbResult))
          .expire(cacheKey(messageservice_sid), 43200)
          .execAsync();
      }
    }
    return dbResult;
  }
};

export default organizationCache;
