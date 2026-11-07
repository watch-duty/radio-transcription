import type { RuleAuditEvent, Tag } from '@transcription/common';

function formatTagsDiff(beforeVal: unknown, afterVal: unknown): string[] {
  const beforeTags = Array.isArray(beforeVal) ? (beforeVal as Tag[]) : [];
  const afterTags = Array.isArray(afterVal) ? (afterVal as Tag[]) : [];
  const beforeStrSet = new Set(beforeTags.map((t) => `${t.key}=${t.value}`));
  const afterStrSet = new Set(afterTags.map((t) => `${t.key}=${t.value}`));

  const added = afterTags
    .filter((t) => !beforeStrSet.has(`${t.key}=${t.value}`))
    .map((t) => `"${t.key}=${t.value}"`);
  const removed = beforeTags
    .filter((t) => !afterStrSet.has(`${t.key}=${t.value}`))
    .map((t) => `"${t.key}=${t.value}"`);

  const tagChanges: string[] = [];
  if (added.length > 0) {
    tagChanges.push(`added ${added.join(', ')}`);
  }
  if (removed.length > 0) {
    tagChanges.push(`removed ${removed.join(', ')}`);
  }

  if (tagChanges.length > 0) {
    return [`Tags: ${tagChanges.join(' and ')}`];
  }
  return [];
}

export function formatDiff(
  before: Record<string, unknown> = {},
  after: Record<string, unknown> = {}
): string[] {
  const changes: string[] = [];
  const keys = Array.from(
    new Set([...Object.keys(before), ...Object.keys(after)])
  );

  keys.forEach((key) => {
    const beforeVal = before[key];
    const afterVal = after[key];

    // Skip if values are equal
    if (JSON.stringify(beforeVal) === JSON.stringify(afterVal)) {
      return;
    }

    if (key === 'tags') {
      changes.push(...formatTagsDiff(beforeVal, afterVal));
      return;
    }

    if (key === 'metadata') {
      return; // metadata updates like updated_at are implicit
    }

    if (beforeVal === undefined || beforeVal === null) {
      changes.push(`${key} set to ${JSON.stringify(afterVal)}`);
    } else if (afterVal === undefined || afterVal === null) {
      changes.push(`${key} cleared (was ${JSON.stringify(beforeVal)})`);
    } else {
      changes.push(
        `${key} changed from ${JSON.stringify(beforeVal)} to ${JSON.stringify(afterVal)}`
      );
    }
  });

  return changes;
}

export interface DateGroupedEvents {
  dateStr: string;
  events: RuleAuditEvent[];
}

export function groupEventsByDate(
  events: RuleAuditEvent[]
): DateGroupedEvents[] {
  const groups: DateGroupedEvents[] = [];
  let currentGroup: DateGroupedEvents | null = null;

  events.forEach((event) => {
    const dateStr = new Date(event.occurredAt).toLocaleDateString([], {
      weekday: 'long',
      month: 'long',
      day: 'numeric',
      year: 'numeric',
    });
    if (!currentGroup || currentGroup.dateStr !== dateStr) {
      currentGroup = { dateStr, events: [] };
      groups.push(currentGroup);
    }
    currentGroup.events.push(event);
  });

  return groups;
}
