import type { Tag } from '@transcription/common';

// Editor rows carry a stable client-side id so React keys and per-row edits/
// deletes don't rely on array position (duplicate keys/values are allowed, so
// neither the index nor the {key,value} pair is a safe identity). The id is
// stripped before sending tags to the API.
export interface TagRow extends Tag {
  id: string;
}

let tagRowIdCounter = 0;
export const nextTagRowId = () => `tag-row-${(tagRowIdCounter += 1)}`;

export const toTagRows = (tags: Tag[]): TagRow[] =>
  tags.map((tag) => ({ ...tag, id: nextTagRowId() }));
