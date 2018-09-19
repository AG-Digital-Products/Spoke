import { r, Message } from '../../models'
import { campaignContactCache } from './campaign-contact'

// QUEUE
// messages-<contactId>

const cacheKey = (contactId) => `${process.env.CACHE_PREFIX||""}messages-${contactId}`

const loadMany = async ({campaignId, contactId}) => {
  if (r.redis) {
  }
}

const dbQuery = ({campaignId, contactId}) => {
  const cols = Object.keys(Message.fields).filter(f => f !== 'service_response').map(f => `message.${f}`)
  let dbQuery =  r.knex('message').select(...cols)
  console.log('message dbquery', contactId, cols)
  if (contactId) {
    dbQuery = dbQuery.where('campaign_contact_id', contactId)
    // TODO: do we need to accomodate active campaigns just after migration here?
    //  probably should include it in the migration
  } else if (campaignId) {
    dbQuery = dbQuery
      .join('assignment', 'message.assignment_id', 'assignment.id')
      .where('assignment.campaign_id', campaignId)
  }
  return dbQuery.orderBy('created_at')
}

const contactIdFromOther = async ({ campaignContactId, assignmentId, cell, service, messageServiceSid }) => {
  if (campaignContactId) {
    console.log('contactIdfromother easy', campaignContactId)
    return campaignContactId
  }
  if (!assignmentId || !cell || !messageServiceSid) {
    throw new Exception('campaignContactId required or assignmentId-cell-service-messageServiceSid triple required')
  }
  if (r.redis) {
    const cellLookup = await campaignContactCache.lookupByCell(
      cell, '', messageServiceSid, /*bailWithoutCache*/ true)
    if (cellLookup) {
      return cellLookup.campaign_contact_id
    }
  }
  // TODO: more ways and by db -- is this necessary if the active-campaign-postmigration edgecase goes away?
}

const cacheDbResult = async (dbResult) => {
  // We assume we are getting a result that is comprehensive for each contact
  if (r.redis) {
    const contacts = {}
    dbResult.forEach(m => {
      if (m.campaign_contact_id in contacts) {
        contacts[m.campaign_contact_id].push(m)
      } else {
        contacts[m.campaign_contact_id] = [m]
      }
    })
    for (const c in contacts) {
      await saveMessageCache(c, contacts[c])
    }
  }
}

const saveMessageCache = async (contactId, contactMessages, justAppend) => {
  if (r.redis) {
    const key = cacheKey(contactId)
    let redisQ = r.redis.multi()
    if (!justAppend) {
      redisQ = redisQ.del(key)
    }
    await redisQ
      .lpush(key, contactMessages.map(m =>JSON.stringify(m)))
      .execAsync()
  }
}

const query = async (query) => {
  // query ~ { campaignContactId, assignmentId, cell, service, messageServiceSid }
  let cid = query.campaignContactId
  console.log('message query', query)
  if (r.redis) {
    cid = await contactIdFromOther(query)
    const [exists, messages] = await r.redis.multi()
      .exists(cacheKey(cid))
      .lrange(cacheKey(cid), 0, -1)
      .execAsync()
    console.log('cached messages exist?', exists, messages)
    if (exists) {
      // note: lrange returns messages in reverse order
      return messages.reverse().map(m => JSON.parse(m))
    }
  }
  const dbResult = await dbQuery({contactId: cid})
  await cacheDbResult(dbResult)
}

export const messageCache = {
  clearQuery: async (query) => {
    if (r.redis) {
      const contactId = await contactIdFromOther(query)
      await r.redis.delAsync(cacheKey(contactId))
    }
  },
  query: query,
  save: async ({ messageInstance, contact }) => {
    // 1. Saves the messageInstance
    // 2. Updates the campaign_contact record with an updated status and updated_at
    // 3. Updates all the related caches
    let contactData = Object.assign({}, contact || {})
    if (messageInstance.is_from_contact) {
      // is_from_contact is a particularly complex conditional
      // This is because we don't have the contact id or other info
      // coming in, but must determine it from cell and messageservice_sid
      const activeCellFound = await campaignContactCache.lookupByCell(
        messageInstance.contact_number,
        messageInstance.service,
        messageInstance.messageservice_sid
      )
      console.log('activeCellFound', activeCellFound)
      if (!activeCellFound) {
        // no active thread to attach message to
        return false
      }
      if (activeCellFound.service_id) {
        // probably in DB non-caching context
        if (messageInstance.service_id === activeCellFound.service_id) {
          // already saved the message -- this is a duplicate message
          console.error('DUPLICATE MESSAGE', messageInstance, activeCellFound)
          return false
        }
      } else {
        // caching context need to look at message thread
        const messageThread = await query({campaignContactId: messageInstance.campaign_contact_id})
        const redundant = messageThread.filter(
          m => (m.service_id && m.service_id === messageInstance.service_id)
        )
        if (redundant.length) {
          console.error('DUPLICATE MESSAGE', messageInstance, activeCellFound)
          return false
        }
      }
      contactData.id = (contactData.id || activeCellFound.campaign_contact_id)
      contactData.timezone_offset = (contactData.timezone_offset || activeCellFound.timezone_offset)
      contactData.message_status = (contactData.message_status || activeCellFound.message_status)
      ['campaign_contact_id', 'assignment_id'].forEach(f => {
        if (!messageInstance[f]) {
          messageInstance[f] = activeCellFound[f]
        }
      })
    }

    await messageInstance.save()
    messageInstance.created_at = new Date()
    await saveMessageCache(contactData.id, [messageInstance], true)
    const newStatus = (messageInstance.is_from_contact
                       ? 'needsResponse'
                       : (contactData.message_status === 'needsResponse'
                          ? 'convo' : 'messaged'))
    await campaignContactCache.updateStatus(
      contactData, newStatus
    )
    if (contact) {
      return { ...contact, message_status: newStatus }
    }
  },
  loadMany: loadMany
}