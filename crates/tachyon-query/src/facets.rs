//! Facet counting (PRD §7.7).
//!
//! ```json
//! { "brand": { "Logitech": 1240, "Razer": 830 } }
//! ```
//!
//! Counts are computed over every document that matched — not the page — so
//! they answer "how many results would remain if I also picked this value",
//! which is the question a facet UI is really asking. That is what PRD §7.7
//! means by "accurate counts after filters".
//!
//! At most [`MAX_FACET_VALUES`] values per field come back, the most common
//! first, since a facet list nobody will scroll is just response weight.

use serde_json::{Map, Number, Value as Json};

use roaring::RoaringBitmap;

use tachyon_core::FieldId;
use tachyon_index::NumKey;

use crate::executor::SearchContext;

/// Facet values returned per field (PRD §7.7: "top 100 facet values").
pub const MAX_FACET_VALUES: usize = 100;

/// Count facet values over a result set.
pub fn compute(
    ctx: &SearchContext,
    fields: &[FieldId],
    matched: &RoaringBitmap,
) -> Map<String, Json> {
    let mut out = Map::new();

    for &field in fields {
        let Some(schema_field) = ctx.schema.field_by_id(field) else {
            continue;
        };

        // A value can occur in more than one source, so counts are summed by
        // value rather than concatenated.
        let mut counts: Vec<(String, u64)> = Vec::new();
        let add = |value: String, count: u64, counts: &mut Vec<(String, u64)>| match counts
            .iter_mut()
            .find(|(existing, _)| *existing == value)
        {
            Some((_, total)) => *total += count,
            None => counts.push((value, count)),
        };

        for source in &ctx.sources {
            let columns = source.columns();

            if let Some(keyword) = columns.keyword(field) {
                for (value, count) in keyword.value_counts_within(matched) {
                    add(value.to_string(), count, &mut counts);
                }
            } else if let Some(numeric) = columns.numeric(field) {
                for (key, count) in numeric.value_counts_within(matched) {
                    add(format_key(key), count, &mut counts);
                }
            }
        }

        // Most common first; ties break on the value so the list is stable.
        counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        counts.truncate(MAX_FACET_VALUES);

        let mut values = Map::new();
        for (value, count) in counts {
            values.insert(value, Json::Number(Number::from(count)));
        }
        out.insert(schema_field.name.clone(), Json::Object(values));
    }

    out
}

/// Numeric facet keys become strings, because JSON object keys are strings.
/// Integers must not pick up a `.0`, or a `bool` facet would read `1.0`.
fn format_key(key: NumKey) -> String {
    match key {
        NumKey::Int(i) => i.to_string(),
        NumKey::Float(f) => f.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tachyon_core::{CollectionSchema, FieldSchema, FieldType, ParsedDocument};
    use tachyon_index::MemTable;

    struct Fixture {
        schema: CollectionSchema,
        memtable: MemTable,
        deleted: RoaringBitmap,
    }

    fn fixture() -> Fixture {
        let schema = CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("year", FieldType::Int).with_facet(true),
                FieldSchema::new("in_stock", FieldType::Bool).with_facet(true),
                FieldSchema::new("tags", FieldType::Keyword).with_facet(true),
            ],
        );
        let mut memtable = MemTable::new(0, &schema);
        for doc in [
            json!({"id": "1", "title": "a", "brand": "Logitech", "year": 2024, "in_stock": true, "tags": ["usb", "wireless"]}),
            json!({"id": "2", "title": "b", "brand": "Razer", "year": 2024, "in_stock": false, "tags": ["usb"]}),
            json!({"id": "3", "title": "c", "brand": "Logitech", "year": 2023, "in_stock": true}),
            json!({"id": "4", "title": "d", "brand": "Logitech", "year": 2024, "in_stock": true}),
        ] {
            memtable.insert(ParsedDocument::parse(doc, &schema).unwrap());
        }
        Fixture { schema, memtable, deleted: RoaringBitmap::new() }
    }

    impl Fixture {
        fn facets(&self, fields: &[FieldId], matched: &RoaringBitmap) -> Map<String, Json> {
            let ctx = SearchContext::new(&self.schema, vec![&self.memtable], &self.deleted);
            compute(&ctx, fields, matched)
        }

        fn all(&self) -> RoaringBitmap {
            (0..4u32).collect()
        }
    }

    #[test]
    fn counts_match_the_prd_shape() {
        let f = fixture();
        let facets = f.facets(&[1], &f.all());
        assert_eq!(facets["brand"], json!({"Logitech": 3, "Razer": 1}));
    }

    #[test]
    fn counts_are_restricted_to_the_result_set() {
        let f = fixture();
        // Only documents 0 and 1 matched.
        let matched = RoaringBitmap::from_iter([0u32, 1]);
        assert_eq!(f.facets(&[1], &matched)["brand"], json!({"Logitech": 1, "Razer": 1}));
        assert_eq!(f.facets(&[1], &RoaringBitmap::new())["brand"], json!({}));
    }

    #[test]
    fn numeric_and_boolean_facets_use_readable_keys() {
        let f = fixture();
        let facets = f.facets(&[2, 3], &f.all());
        assert_eq!(facets["year"], json!({"2024": 3, "2023": 1}));
        // A bool must not come back as `1.0`.
        assert_eq!(facets["in_stock"], json!({"1": 3, "0": 1}));
    }

    #[test]
    fn multi_valued_fields_count_once_per_value() {
        let f = fixture();
        assert_eq!(f.facets(&[4], &f.all())["tags"], json!({"usb": 2, "wireless": 1}));
    }

    #[test]
    fn several_fields_can_be_faceted_at_once() {
        let f = fixture();
        let facets = f.facets(&[1, 2], &f.all());
        assert_eq!(facets.len(), 2);
        assert!(facets.contains_key("brand") && facets.contains_key("year"));
    }

    #[test]
    fn values_come_back_most_common_first() {
        let f = fixture();
        let facets = f.facets(&[1], &f.all());
        let counts: Vec<u64> =
            facets["brand"].as_object().unwrap().values().map(|v| v.as_u64().unwrap()).collect();
        assert!(counts.windows(2).all(|w| w[0] >= w[1]), "not descending: {counts:?}");
    }

    #[test]
    fn no_facet_fields_means_no_facets() {
        let f = fixture();
        assert!(f.facets(&[], &f.all()).is_empty());
    }

    #[test]
    fn only_the_top_hundred_values_are_returned() {
        let schema = CollectionSchema::new(
            "wide",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("sku", FieldType::Keyword).with_facet(true),
            ],
        );
        let mut memtable = MemTable::new(0, &schema);
        for i in 0..(MAX_FACET_VALUES + 50) {
            memtable.insert(
                ParsedDocument::parse(
                    json!({"id": i.to_string(), "title": "x", "sku": format!("sku-{i:04}")}),
                    &schema,
                )
                .unwrap(),
            );
        }
        let f = Fixture { schema, memtable, deleted: RoaringBitmap::new() };
        let matched: RoaringBitmap = (0..(MAX_FACET_VALUES + 50) as u32).collect();
        let facets = f.facets(&[1], &matched);
        assert_eq!(facets["sku"].as_object().unwrap().len(), MAX_FACET_VALUES);
    }
}
