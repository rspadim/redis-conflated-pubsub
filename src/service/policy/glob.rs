#[derive(Clone, Debug)]
enum GlobToken {
    Star,
    Any,
    Literal(u8),
    Class {
        negated: bool,
        ranges: Vec<(u8, u8)>,
    },
}

pub(in crate::service) struct GlobPattern {
    tokens: Vec<GlobToken>,
}

impl GlobPattern {
    pub(in crate::service) fn parse(pattern: &str) -> Self {
        Self {
            tokens: parse_glob(pattern),
        }
    }

    pub(in crate::service) fn matches(&self, value: &str, workspace: &mut GlobWorkspace) -> bool {
        glob_matches_tokens(&self.tokens, value, workspace)
    }

    #[cfg(test)]
    pub(super) fn estimated_heap_bytes(&self) -> usize {
        self.tokens.capacity() * std::mem::size_of::<GlobToken>()
            + self
                .tokens
                .iter()
                .map(|token| match token {
                    GlobToken::Class { ranges, .. } => {
                        ranges.capacity() * std::mem::size_of::<(u8, u8)>()
                    }
                    _ => 0,
                })
                .sum::<usize>()
    }
}

#[derive(Default)]
pub(in crate::service) struct GlobWorkspace {
    previous: Vec<bool>,
    current: Vec<bool>,
}

#[cfg(test)]
pub(super) fn glob_matches(pattern: &str, value: &str) -> bool {
    GlobPattern::parse(pattern).matches(value, &mut GlobWorkspace::default())
}

fn glob_matches_tokens(tokens: &[GlobToken], value: &str, workspace: &mut GlobWorkspace) -> bool {
    let value = value.as_bytes();
    workspace.previous.resize(value.len() + 1, false);
    workspace.current.resize(value.len() + 1, false);
    workspace.previous.fill(false);
    workspace.current.fill(false);
    workspace.previous[0] = true;

    for token in tokens {
        workspace.current.fill(false);
        match token {
            GlobToken::Star => {
                workspace.current[0] = workspace.previous[0];
                for index in 1..=value.len() {
                    workspace.current[index] =
                        workspace.previous[index] || workspace.current[index - 1];
                }
            }
            GlobToken::Any => {
                workspace.current[1..].copy_from_slice(&workspace.previous[..value.len()]);
            }
            GlobToken::Literal(expected) => {
                for (index, actual) in value.iter().enumerate() {
                    workspace.current[index + 1] =
                        workspace.previous[index] && *actual == *expected;
                }
            }
            GlobToken::Class { negated, ranges } => {
                for (index, actual) in value.iter().enumerate() {
                    let included = ranges
                        .iter()
                        .any(|(start, end)| start <= actual && actual <= end);
                    workspace.current[index + 1] =
                        workspace.previous[index] && (included != *negated);
                }
            }
        }
        std::mem::swap(&mut workspace.previous, &mut workspace.current);
    }
    workspace.previous[value.len()]
}

fn parse_glob(pattern: &str) -> Vec<GlobToken> {
    let bytes = pattern.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'*' => {
                if !matches!(tokens.last(), Some(GlobToken::Star)) {
                    tokens.push(GlobToken::Star);
                }
                index += 1;
            }
            b'?' => {
                tokens.push(GlobToken::Any);
                index += 1;
            }
            b'\\' if index + 1 < bytes.len() => {
                tokens.push(GlobToken::Literal(bytes[index + 1]));
                index += 2;
            }
            b'[' => {
                if let Some((class, after)) = parse_glob_class(bytes, index) {
                    tokens.push(class);
                    index = after;
                } else {
                    tokens.push(GlobToken::Literal(b'['));
                    index += 1;
                }
            }
            literal => {
                tokens.push(GlobToken::Literal(literal));
                index += 1;
            }
        }
    }
    tokens
}

fn parse_glob_class(bytes: &[u8], start: usize) -> Option<(GlobToken, usize)> {
    let mut index = start + 1;
    let negated = bytes.get(index) == Some(&b'^');
    if negated {
        index += 1;
    }
    let mut elements = Vec::<(u8, bool)>::new();
    let mut closed = false;
    while index < bytes.len() {
        match bytes[index] {
            b']' if !elements.is_empty() => {
                closed = true;
                index += 1;
                break;
            }
            b'\\' if index + 1 < bytes.len() => {
                elements.push((bytes[index + 1], true));
                index += 2;
            }
            byte => {
                elements.push((byte, false));
                index += 1;
            }
        }
    }
    if !closed || elements.is_empty() {
        return None;
    }

    let mut ranges = Vec::new();
    let mut element_index = 0;
    while element_index < elements.len() {
        if element_index + 2 < elements.len() && elements[element_index + 1] == (b'-', false) {
            ranges.push((elements[element_index].0, elements[element_index + 2].0));
            element_index += 3;
        } else {
            let character = elements[element_index].0;
            ranges.push((character, character));
            element_index += 1;
        }
    }
    Some((GlobToken::Class { negated, ranges }, index))
}
