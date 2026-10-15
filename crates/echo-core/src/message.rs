//! Outbound message construction.

use crate::segment::Segment;

/// Builder for chains of message segments.
#[derive(Debug, Clone, Default)]
pub struct MessageBuilder {
    segments: Vec<Segment>,
}

impl MessageBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an arbitrary segment.
    pub fn push(mut self, segment: Segment) -> Self {
        self.segments.push(segment);
        self
    }

    pub fn text(mut self, content: impl Into<String>) -> Self {
        self.segments.push(Segment::text(content));
        self
    }

    pub fn at(mut self, qq: impl Into<String>) -> Self {
        self.segments.push(Segment::at(qq));
        self
    }

    pub fn at_all(mut self) -> Self {
        self.segments.push(Segment::at_all());
        self
    }

    pub fn image(mut self, file: impl Into<String>) -> Self {
        self.segments.push(Segment::image(file));
        self
    }

    pub fn face(mut self, id: impl Into<String>) -> Self {
        self.segments.push(Segment::face(id));
        self
    }

    pub fn reply(mut self, message_id: impl Into<String>) -> Self {
        self.segments.push(Segment::reply(message_id));
        self
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn build(self) -> Vec<Segment> {
        self.segments
    }
}

/// A quick text-only message.
pub fn text(content: impl Into<String>) -> Vec<Segment> {
    vec![Segment::text(content)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::KnownSegment;

    #[test]
    fn build_message_chain() {
        let segs = MessageBuilder::new()
            .reply("9001")
            .at("20001")
            .text("hi")
            .image("base64://abc")
            .build();

        assert_eq!(segs.len(), 4);
        assert!(matches!(
            segs[0],
            Segment::Known(KnownSegment::Reply { .. })
        ));
        assert!(matches!(segs[1], Segment::Known(KnownSegment::At { .. })));
        assert!(matches!(segs[2], Segment::Known(KnownSegment::Text { .. })));
        assert!(matches!(
            segs[3],
            Segment::Known(KnownSegment::Image { .. })
        ));
    }

    #[test]
    fn quick_text() {
        assert_eq!(text("hi"), vec![Segment::text("hi")]);
    }

    #[test]
    fn builder_defaults_to_empty() {
        let b = MessageBuilder::new();
        assert!(b.is_empty());
        assert!(b.build().is_empty());
    }

    #[test]
    fn builder_at_all_and_face() {
        let segs = MessageBuilder::new().at_all().face("1").build();
        assert_eq!(segs.len(), 2);
        match &segs[0] {
            Segment::Known(KnownSegment::At { data }) => assert_eq!(data.qq, "all"),
            other => panic!("unexpected: {other:?}"),
        }
        match &segs[1] {
            Segment::Known(KnownSegment::Face { data }) => assert_eq!(data.id, "1"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn builder_chains_are_immutable() {
        // push() consumes self — chaining must not affect earlier builders.
        let base = MessageBuilder::new().text("a");
        let extended = base.clone().at("1");
        assert_eq!(base.build().len(), 1);
        assert_eq!(extended.build().len(), 2);
    }
}
