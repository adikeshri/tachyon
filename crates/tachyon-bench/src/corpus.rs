//! Synthetic e-commerce corpus.
//!
//! Real relevance work needs a real corpus, but a *performance* benchmark
//! mostly needs the right shape: a Zipf-ish term distribution (a few words in
//! most documents, most words in few), realistic field lengths, and enough
//! distinct values to make facets and filters do work. Generating it means the
//! benchmark runs anywhere with no download step.
//!
//! The generator is a deterministic PRNG, so two runs on the same machine
//! compare like for like.

use serde_json::{json, Value};

use tachyon_core::{CollectionSchema, FieldSchema, FieldType};

/// xorshift64*, chosen because it is four lines and reproducible. Nothing here
/// needs cryptographic randomness.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }

    fn pick<'a, T>(&mut self, options: &'a [T]) -> &'a T {
        &options[self.below(options.len())]
    }

    /// Index into `n` items, biased towards the front — a cheap stand-in for a
    /// Zipf distribution, so common words really are common.
    fn zipfish(&mut self, n: usize) -> usize {
        let a = self.below(n);
        let b = self.below(n);
        a.min(b)
    }
}

const ADJECTIVES: &[&str] = &[
    "wireless",
    "wired",
    "mechanical",
    "ergonomic",
    "silent",
    "compact",
    "portable",
    "premium",
    "rugged",
    "slim",
    "gaming",
    "professional",
    "vintage",
    "modular",
    "waterproof",
];

const NOUNS: &[&str] = &[
    "mouse",
    "keyboard",
    "monitor",
    "headset",
    "webcam",
    "microphone",
    "speaker",
    "hub",
    "charger",
    "cable",
    "adapter",
    "stand",
    "dock",
    "controller",
    "tablet",
];

const MATERIALS: &[&str] =
    &["aluminium", "carbon", "plastic", "steel", "bamboo", "leather", "silicone", "titanium"];

const QUALIFIERS: &[&str] = &[
    "for the office",
    "with backlight",
    "for travel",
    "with usb c",
    "for creators",
    "with noise cancelling",
    "for small desks",
    "with fast charging",
];

pub const BRANDS: &[&str] = &[
    "Logitech",
    "Razer",
    "Anker",
    "Corsair",
    "Keychron",
    "Belkin",
    "Elgato",
    "SteelSeries",
    "HyperX",
    "Ugreen",
];

pub const CATEGORIES: &[&str] =
    &["peripherals", "audio", "power", "display", "accessories", "storage"];

/// The benchmark schema: the PRD §7.1 example, widened enough to exercise
/// filters, sorting, and facets at once.
pub fn schema(name: &str) -> CollectionSchema {
    CollectionSchema::new(
        name,
        vec![
            FieldSchema::new("title", FieldType::Text).required(),
            FieldSchema::new("description", FieldType::Text),
            FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
            FieldSchema::new("category", FieldType::Keyword).with_facet(true),
            FieldSchema::new("price", FieldType::Int).with_filter(true).with_sort(true),
            FieldSchema::new("rating", FieldType::Float).with_filter(true).with_sort(true),
            FieldSchema::new("popularity", FieldType::Int).with_sort(true),
        ],
    )
}

/// Generate one document.
pub fn document(rng: &mut Rng, id: usize) -> Value {
    let adjective = ADJECTIVES[rng.zipfish(ADJECTIVES.len())];
    let noun = NOUNS[rng.zipfish(NOUNS.len())];
    let material = rng.pick(MATERIALS);
    let qualifier = rng.pick(QUALIFIERS);

    let title = format!("{adjective} {noun}");
    let description = format!(
        "A {material} {adjective} {noun} {qualifier}. Built for everyday use and backed by a \
         two year warranty."
    );

    json!({
        "id": id.to_string(),
        "title": title,
        "description": description,
        "brand": rng.pick(BRANDS),
        "category": rng.pick(CATEGORIES),
        "price": (rng.below(50_000) + 500) as i64,
        "rating": (rng.below(50) as f64) / 10.0,
        "popularity": rng.below(10_000) as i64,
    })
}

/// Queries a benchmark should ask: single words, pairs, a phrase, a typo, and
/// a prefix — the shapes real traffic actually contains.
pub fn queries(rng: &mut Rng, count: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            let adjective = ADJECTIVES[rng.zipfish(ADJECTIVES.len())];
            let noun = NOUNS[rng.zipfish(NOUNS.len())];
            match i % 5 {
                0 => noun.to_string(),
                1 => format!("{adjective} {noun}"),
                2 => format!("\"{adjective} {noun}\""),
                // A transposed pair of characters, the commonest real typo.
                3 => transpose(adjective),
                _ => adjective[..adjective.len().min(4)].to_string(),
            }
        })
        .collect()
}

fn transpose(word: &str) -> String {
    let mut chars: Vec<char> = word.chars().collect();
    if chars.len() >= 4 {
        chars.swap(1, 2);
    }
    chars.into_iter().collect()
}
