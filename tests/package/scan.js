'use strict';

// Not a *.test.js: the scanner the convention guards in consistency.test.js
// read src/ with.

// Code with everything that is not code blanked out — comments, the text of
// strings and templates, regular expressions — positions and line breaks
// kept, so a pattern can be matched against code alone.
const codeOf = (text) => {
  const out = text.split('');
  const blank = (from, to) => {
    for (let i = from; i < to; i++) if (out[i] !== '\n') out[i] = ' ';
  };
  // What a `/` may follow when it opens a regular expression.
  const REGEX_AFTER = /(?:^|[(,=:[!&|?{};+\-*%<>~^]|\breturn|\btypeof|\bcase|\bof|\bin|=>)\s*$/;
  const templates = []; // brace depth at which each open template resumes
  let depth = 0;
  let i = 0;
  const template = (start) => {
    // From just after a backtick (or after a `}` closing an interpolation).
    let j = start;
    while (j < text.length) {
      if (text[j] === '\\') j += 2;
      else if (text[j] === '`') {
        blank(start, j);
        return j + 1;
      } else if (text[j] === '$' && text[j + 1] === '{') {
        blank(start, j);
        templates.push(depth);
        depth++;
        return j + 2;
      } else j++;
    }
    return j;
  };
  while (i < text.length) {
    const char = text[i];
    const next = text[i + 1];
    if (char === '/' && next === '/') {
      let j = i;
      while (j < text.length && text[j] !== '\n') j++;
      blank(i, j);
      i = j;
    } else if (char === '/' && next === '*') {
      const end = text.indexOf('*/', i + 2);
      const j = end === -1 ? text.length : end + 2;
      blank(i, j);
      i = j;
    } else if (char === "'" || char === '"') {
      let j = i + 1;
      while (j < text.length && text[j] !== char && text[j] !== '\n') j += text[j] === '\\' ? 2 : 1;
      blank(i + 1, j);
      i = j + 1;
    } else if (char === '`') {
      i = template(i + 1);
    } else if (char === '/' && REGEX_AFTER.test(text.slice(Math.max(0, i - 40), i))) {
      let j = i + 1;
      let inClass = false;
      while (j < text.length && text[j] !== '\n') {
        if (text[j] === '\\') j++;
        else if (text[j] === '[') inClass = true;
        else if (text[j] === ']') inClass = false;
        else if (text[j] === '/' && !inClass) break;
        j++;
      }
      blank(i + 1, j);
      i = j + 1;
    } else if (char === '{') {
      depth++;
      i++;
    } else if (char === '}') {
      depth--;
      i++;
      if (templates.length > 0 && templates.at(-1) === depth) {
        templates.pop();
        i = template(i);
      }
    } else i++;
  }
  return out.join('');
};

// Every `...` that spreads into the arguments of a call: its innermost open
// bracket is a `(` that is not a parameter list (one followed by `=>` or by
// the `{` of a body).
const callSpreads = (code) => {
  const found = [];
  const stack = [];
  for (let i = 0; i < code.length; i++) {
    const char = code[i];
    if (char === '(' || char === '[' || char === '{') stack.push({ char, spreads: [] });
    else if (char === ')' || char === ']' || char === '}') {
      const open = stack.pop();
      if (open === undefined || open.char !== '(' || open.spreads.length === 0) continue;
      const after = code.slice(i + 1, i + 40).trimStart();
      if (after.startsWith('=>') || after.startsWith('{')) continue;
      for (const at of open.spreads) found.push(at);
    } else if (char === '.' && code[i + 1] === '.' && code[i + 2] === '.') {
      if (stack.length > 0) stack.at(-1).spreads.push(i);
      i += 2;
    }
  }
  return found.sort((a, b) => a - b);
};
module.exports = { codeOf, callSpreads };
