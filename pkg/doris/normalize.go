package doris

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	defaultIcebergCatalog      = "azra_lake"
	coupledStorageResourceProp = "storage_vault_name"
	coupledFileCacheTTLProp    = "file_cache_ttl_seconds"
	unknownPropertiesErrPrefix = "unknown properties: {"
)

var (
	tenantChainIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)
	createChainTableRe = regexp.MustCompile("(?i)^\\s*CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`?(internal|azra_lake)`?\\s*\\.\\s*`?tenant_\\$\\{TENANT_SLUG\\}`?\\s*\\.\\s*`?([a-zA-Z_][a-zA-Z0-9_]*)`?\\b")
	createChainViewRe  = regexp.MustCompile("(?i)^\\s*CREATE\\s+(?:OR\\s+REPLACE\\s+)?VIEW\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`?(internal)`?\\s*\\.\\s*`?tenant_\\$\\{TENANT_SLUG\\}`?\\s*\\.\\s*`?([a-zA-Z_][a-zA-Z0-9_]*)`?\\b")
	modifyChainViewRe  = regexp.MustCompile("(?i)^\\s*(?:ALTER\\s+VIEW|DROP\\s+VIEW(?:\\s+IF\\s+EXISTS)?)\\s+`?(internal)`?\\s*\\.\\s*`?tenant_\\$\\{TENANT_SLUG\\}`?\\s*\\.\\s*`?([a-zA-Z_][a-zA-Z0-9_]*)`?\\b")
	modifyChainTableRe = regexp.MustCompile("(?i)^\\s*(?:ALTER\\s+TABLE|DROP\\s+TABLE(?:\\s+IF\\s+EXISTS)?|TRUNCATE\\s+TABLE|INSERT\\s+(?:OVERWRITE\\s+TABLE|INTO)|REPLACE\\s+INTO|UPDATE|DELETE\\s+FROM)\\s+`?(internal|azra_lake)`?\\s*\\.\\s*`?tenant_\\$\\{TENANT_SLUG\\}`?\\s*\\.\\s*`?([a-zA-Z_][a-zA-Z0-9_]*)`?\\b")
	indexChainTableRe  = regexp.MustCompile("(?i)^\\s*(?:BUILD\\s+INDEX|DROP\\s+INDEX(?:\\s+IF\\s+EXISTS)?|CREATE\\s+INDEX(?:\\s+IF\\s+NOT\\s+EXISTS)?)\\s+`?[a-zA-Z_][a-zA-Z0-9_]*`?\\s+ON\\s+`?(internal|azra_lake)`?\\s*\\.\\s*`?tenant_\\$\\{TENANT_SLUG\\}`?\\s*\\.\\s*`?([a-zA-Z_][a-zA-Z0-9_]*)`?\\b")
	forbiddenChainRe   = regexp.MustCompile(`(?i)^\s*(?:GRANT|REVOKE|DROP\s+(?:DATABASE|SCHEMA|CATALOG|USER|ROLE)|CREATE\s+(?:DATABASE|SCHEMA|CATALOG|USER|ROLE)|ALTER\s+(?:DATABASE|SCHEMA|CATALOG|USER|ROLE)|SET|USE|ADMIN|KILL|SHOW)\b`)
	threePartRefRe     = regexp.MustCompile("`?([a-zA-Z_][a-zA-Z0-9_]*)`?\\s*\\.\\s*`?(tenant_\\$\\{TENANT_SLUG\\}|[a-zA-Z_][a-zA-Z0-9_]*)`?\\s*\\.\\s*`?([a-zA-Z_][a-zA-Z0-9_]*)`?")
)

// ValidateTenantChainStatement verifies that stmt is a tenant-isolated DDL/DML statement
// targeting `<internal|azra_lake>.tenant_${TENANT_SLUG}.<owned_table_or_view>`, rejects
// administrative/privilege statements, registers newly created tables and views in seenTables,
// seenViews, and ownedTables, and ensures all 3-part object references stay within owned objects.
func ValidateTenantChainStatement(stmt string, seenTables, seenViews map[string]bool, ownedTables *[]string) error {
	if seenTables == nil {
		seenTables = make(map[string]bool)
	}
	if seenViews == nil {
		seenViews = make(map[string]bool)
	}

	stripped := stripSQLCommentsAndStringLiterals(stmt)
	if forbiddenChainRe.MatchString(stripped) {
		return errors.New("forbidden administrative or privilege statement in migration chain")
	}

	switch {
	case createChainTableRe.MatchString(stripped):
		m := createChainTableRe.FindStringSubmatch(stripped)
		tbl := strings.ToLower(strings.TrimSpace(m[2]))
		if !isValidTenantChainIdent(tbl) {
			return fmt.Errorf("invalid table identifier %q", tbl)
		}
		if !seenTables[tbl] {
			seenTables[tbl] = true
			if ownedTables != nil {
				*ownedTables = append(*ownedTables, tbl)
			}
		}
	case createChainViewRe.MatchString(stripped):
		m := createChainViewRe.FindStringSubmatch(stripped)
		vw := strings.ToLower(strings.TrimSpace(m[2]))
		if !isValidTenantChainIdent(vw) {
			return fmt.Errorf("invalid view identifier %q", vw)
		}
		seenViews[vw] = true
	case modifyChainViewRe.MatchString(stripped):
		m := modifyChainViewRe.FindStringSubmatch(stripped)
		vw := strings.ToLower(strings.TrimSpace(m[2]))
		if !seenViews[vw] {
			return fmt.Errorf("statement targets unowned view %q", vw)
		}
	case modifyChainTableRe.MatchString(stripped):
		m := modifyChainTableRe.FindStringSubmatch(stripped)
		tbl := strings.ToLower(strings.TrimSpace(m[2]))
		if !seenTables[tbl] {
			return fmt.Errorf("statement targets unowned table %q", tbl)
		}
	case indexChainTableRe.MatchString(stripped):
		m := indexChainTableRe.FindStringSubmatch(stripped)
		tbl := strings.ToLower(strings.TrimSpace(m[2]))
		if !seenTables[tbl] {
			return fmt.Errorf("index statement targets unowned table %q", tbl)
		}
	default:
		return errors.New("statement does not target <internal|azra_lake>.tenant_${TENANT_SLUG}.<owned_table>")
	}

	refs := threePartRefRe.FindAllStringSubmatch(stripped, -1)
	if len(refs) == 0 {
		return errors.New("statement is missing <catalog>.tenant_${TENANT_SLUG}.<table> qualification")
	}
	for _, ref := range refs {
		cat := strings.ToLower(strings.TrimSpace(ref[1]))
		db := strings.TrimSpace(ref[2])
		obj := strings.ToLower(strings.TrimSpace(ref[3]))
		if cat != "internal" && cat != "azra_lake" {
			return fmt.Errorf("statement references disallowed catalog %q", ref[1])
		}
		if db != "tenant_${TENANT_SLUG}" {
			return fmt.Errorf("statement references disallowed database %q", db)
		}
		if !seenTables[obj] && !seenViews[obj] {
			return fmt.Errorf("statement references unowned object %q", obj)
		}
	}
	return nil
}

func isValidTenantChainIdent(name string) bool {
	return tenantChainIdentRe.MatchString(name)
}

func stripSQLCommentsAndStringLiterals(stmt string) string {
	var b strings.Builder
	b.Grow(len(stmt))
	n := len(stmt)
	for i := 0; i < n; {
		ch := stmt[i]
		if ch == '\'' || ch == '"' {
			quote := ch
			b.WriteByte(quote)
			i++
			for i < n {
				c := stmt[i]
				if c == '\\' && i+1 < n {
					i += 2
					continue
				}
				i++
				if c == quote {
					if i < n && stmt[i] == quote {
						i++
						continue
					}
					break
				}
			}
			b.WriteByte(quote)
			continue
		}
		if ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(stmt, i)
			if !ok {
				b.WriteString(stmt[i:])
				break
			}
			b.WriteString(stmt[i:nextQuote])
			i = nextQuote
			continue
		}
		if ch == '-' && i+1 < n && stmt[i+1] == '-' {
			i += 2
			for i < n && stmt[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
			continue
		}
		if ch == '#' {
			i++
			for i < n && stmt[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
			continue
		}
		if ch == '/' && i+1 < n && stmt[i+1] == '*' {
			end := strings.Index(stmt[i+2:], "*/")
			if end < 0 {
				break
			}
			i += 2 + end + 2
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(ch)
		i++
	}
	return b.String()
}

func isCoupledModeDisallowedProp(key string) bool {
	return strings.EqualFold(key, coupledStorageResourceProp) ||
		strings.EqualFold(key, coupledFileCacheTTLProp)
}

// IsCoupledModeUnknownPropertiesErr reports whether err is a Doris Error 1105
// "Unknown properties: {...}" rejection containing only cloud-decoupled properties
// ("storage_vault_name" and/or "file_cache_ttl_seconds").
func IsCoupledModeUnknownPropertiesErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	idx := strings.Index(lower, unknownPropertiesErrPrefix)
	if idx < 0 {
		return false
	}
	rest := msg[idx+len(unknownPropertiesErrPrefix):]
	closeIdx := strings.IndexByte(rest, '}')
	if closeIdx <= 0 {
		return false
	}
	inside := strings.TrimSpace(rest[:closeIdx])
	if inside == "" {
		return false
	}
	entries := strings.Split(inside, ",")
	for _, rawEntry := range entries {
		entry := strings.TrimSpace(rawEntry)
		if entry == "" {
			return false
		}
		eq := strings.IndexByte(entry, '=')
		if eq <= 0 {
			return false
		}
		key := strings.Trim(strings.TrimSpace(entry[:eq]), `"'`)
		if !isCoupledModeDisallowedProp(key) {
			return false
		}
	}
	return true
}

type spanReplacement struct {
	start int
	end   int
	text  string
}

// StripCoupledModeCreateTableProperties strips cloud-only decoupled storage-tier Doris table properties
// from top-level PROPERTIES (...) clauses of CREATE TABLE statements so coupled-mode Doris deployments
// accept shared migration chains without altering source checksums.
func StripCoupledModeCreateTableProperties(stmt string) string {
	if !isCreateTableStatement(stmt) {
		return stmt
	}
	n := len(stmt)
	depth := 0
	var repls []spanReplacement

	for i := 0; i < n; {
		nextTrivia, ok := skipSQLTrivia(stmt, i)
		if !ok {
			return stmt
		}
		if nextTrivia > i {
			i = nextTrivia
			continue
		}
		ch := stmt[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(stmt, i)
			if !ok {
				return stmt
			}
			i = nextQuote
			continue
		}
		if ch == '(' {
			depth++
			i++
			continue
		}
		if ch == ')' {
			if depth > 0 {
				depth--
			}
			i++
			continue
		}
		if depth == 0 && isSQLIdentStart(ch) && (i == 0 || !isSQLIdentPart(stmt[i-1])) {
			word, wordEnd, _ := readSQLWord(stmt, i)
			if strings.EqualFold(word, "PROPERTIES") {
				openParen, ok := skipSQLTrivia(stmt, wordEnd)
				if !ok {
					return stmt
				}
				if openParen < n && stmt[openParen] == '(' {
					closeParen, ok := findMatchingCloseParen(stmt, openParen)
					if !ok {
						return stmt
					}
					inner := stmt[openParen+1 : closeParen]
					rewrittenInner, allStripped, changed := stripCoupledPropsInner(inner)
					if changed {
						if allStripped {
							prevEnd := 0
							if len(repls) > 0 {
								prevEnd = repls[len(repls)-1].end
							}
							trimStart := i
							for trimStart > prevEnd && isSQLSpace(stmt[trimStart-1]) {
								trimStart--
							}
							repText := ""
							if trimStart < i && closeParen+1 < n && !isSQLSpace(stmt[closeParen+1]) && stmt[closeParen+1] != ';' {
								repText = " "
							}
							repls = append(repls, spanReplacement{start: trimStart, end: closeParen + 1, text: repText})
						} else {
							repls = append(repls, spanReplacement{start: openParen + 1, end: closeParen, text: rewrittenInner})
						}
					}
					i = closeParen + 1
					continue
				}
			}
			i = wordEnd
			continue
		}
		i++
	}

	if len(repls) == 0 {
		return stmt
	}
	return applySpanReplacements(stmt, repls)
}

func applySpanReplacements(s string, repls []spanReplacement) string {
	if len(repls) == 0 {
		return s
	}
	var out strings.Builder
	out.Grow(len(s))
	cursor := 0
	for _, r := range repls {
		out.WriteString(s[cursor:r.start])
		out.WriteString(r.text)
		cursor = r.end
	}
	out.WriteString(s[cursor:])
	return out.String()
}

type sqlSpan struct {
	start int
	end   int
}

func splitTopLevelSQLItemSpans(s string) ([]sqlSpan, bool) {
	n := len(s)
	var spans []sqlSpan
	depth := 0
	itemStart := 0
	for i := 0; i < n; {
		nextTrivia, ok := skipSQLTrivia(s, i)
		if !ok {
			return nil, false
		}
		if nextTrivia > i {
			i = nextTrivia
			continue
		}
		ch := s[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(s, i)
			if !ok {
				return nil, false
			}
			i = nextQuote
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return nil, false
			}
			depth--
		case ',':
			if depth == 0 {
				spans = append(spans, sqlSpan{start: itemStart, end: i})
				itemStart = i + 1
			}
		}
		i++
	}
	if depth != 0 {
		return nil, false
	}
	spans = append(spans, sqlSpan{start: itemStart, end: n})
	return spans, true
}

func splitTopLevelSQLItems(s string) ([]string, bool) {
	spans, ok := splitTopLevelSQLItemSpans(s)
	if !ok {
		return nil, false
	}
	items := make([]string, len(spans))
	for i, sp := range spans {
		items[i] = s[sp.start:sp.end]
	}
	return items, true
}

func skipSQLIdentToken(s string, pos int) (int, bool) {
	if pos >= len(s) {
		return pos, false
	}
	if s[pos] == '`' {
		return skipSQLQuotedOrBacktick(s, pos)
	}
	_, end, ok := readSQLWord(s, pos)
	return end, ok
}

func readSQLCatalogOrTargetSegment(s string, pos int) (string, int, bool) {
	n := len(s)
	if pos >= n {
		return "", pos, false
	}
	if s[pos] == '`' {
		end, ok := skipSQLQuotedOrBacktick(s, pos)
		if !ok || end < pos+2 {
			return "", pos, false
		}
		return strings.ReplaceAll(s[pos+1:end-1], "``", "`"), end, true
	}
	if !isSQLIdentStart(s[pos]) && s[pos] != '$' {
		return "", pos, false
	}
	end := pos
	for end < n {
		c := s[end]
		if isSQLIdentPart(c) || c == '{' || c == '}' {
			end++
			continue
		}
		break
	}
	return s[pos:end], end, end > pos
}

func readThreePartTableRef(s string, pos int) (string, int, bool) {
	p, ok := skipSQLTrivia(s, pos)
	if !ok {
		return "", pos, false
	}
	cat, next, ok := readSQLCatalogOrTargetSegment(s, p)
	if !ok {
		return "", pos, false
	}
	p, ok = skipSQLTrivia(s, next)
	if !ok || p >= len(s) || s[p] != '.' {
		return "", pos, false
	}
	p, ok = skipSQLTrivia(s, p+1)
	if !ok {
		return "", pos, false
	}
	_, next, ok = readSQLCatalogOrTargetSegment(s, p)
	if !ok {
		return "", pos, false
	}
	p, ok = skipSQLTrivia(s, next)
	if !ok || p >= len(s) || s[p] != '.' {
		return "", pos, false
	}
	p, ok = skipSQLTrivia(s, p+1)
	if !ok {
		return "", pos, false
	}
	_, next, ok = readSQLCatalogOrTargetSegment(s, p)
	if !ok {
		return "", pos, false
	}
	return cat, next, true
}

func isTableConstraintLeadWord(word string) bool {
	switch strings.ToUpper(word) {
	case "CONSTRAINT", "PRIMARY", "UNIQUE", "INDEX", "KEY", "FOREIGN", "CHECK":
		return true
	default:
		return false
	}
}

func consumeOptionalIntDisplayWidth(s string, pos int) int {
	p := pos
	for p < len(s) && (s[p] == ' ' || s[p] == '\t') {
		p++
	}
	if p >= len(s) || s[p] != '(' {
		return pos
	}
	closeParen, ok := findMatchingCloseParen(s, p)
	if !ok {
		return pos
	}
	digits := strings.TrimSpace(s[p+1 : closeParen])
	if digits == "" {
		return pos
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return pos
		}
	}
	return closeParen + 1
}

func widenIcebergIntTypesInSpec(spec string) (string, bool) {
	n := len(spec)
	var repls []spanReplacement
	for i := 0; i < n; {
		nextTrivia, ok := skipSQLTrivia(spec, i)
		if !ok {
			return spec, false
		}
		if nextTrivia > i {
			i = nextTrivia
			continue
		}
		ch := spec[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(spec, i)
			if !ok {
				return spec, false
			}
			i = nextQuote
			continue
		}
		if isSQLIdentStart(ch) && (i == 0 || !isSQLIdentPart(spec[i-1])) {
			word, wordEnd, _ := readSQLWord(spec, i)
			if strings.EqualFold(word, "AFTER") {
				afterPos, ok := skipSQLTrivia(spec, wordEnd)
				if !ok {
					return spec, false
				}
				if afterEnd, ok := skipSQLIdentToken(spec, afterPos); ok {
					i = afterEnd
					continue
				}
			}
			if strings.EqualFold(word, "TINYINT") || strings.EqualFold(word, "SMALLINT") {
				repEnd := consumeOptionalIntDisplayWidth(spec, wordEnd)
				repls = append(repls, spanReplacement{start: i, end: repEnd, text: "INT"})
				i = repEnd
				continue
			}
			i = wordEnd
			continue
		}
		i++
	}
	if len(repls) == 0 {
		return spec, false
	}
	return applySpanReplacements(spec, repls), true
}

func rewriteIcebergColumnDefinitionsBlock(inner string) (string, bool) {
	spans, ok := splitTopLevelSQLItemSpans(inner)
	if !ok {
		return inner, false
	}
	var repls []spanReplacement
	for _, sp := range spans {
		colItem := inner[sp.start:sp.end]
		p, ok := skipSQLTrivia(colItem, 0)
		if !ok || p >= len(colItem) {
			continue
		}
		if colItem[p] != '`' {
			if leadWord, _, wordOK := readSQLWord(colItem, p); wordOK && isTableConstraintLeadWord(leadWord) {
				continue
			}
		}
		typeStart, ok := skipSQLIdentToken(colItem, p)
		if !ok || typeStart >= len(colItem) {
			continue
		}
		rewrittenSpec, changed := widenIcebergIntTypesInSpec(colItem[typeStart:])
		if changed {
			repls = append(repls, spanReplacement{
				start: sp.start + typeStart,
				end:   sp.end,
				text:  rewrittenSpec,
			})
		}
	}
	if len(repls) == 0 {
		return inner, false
	}
	return applySpanReplacements(inner, repls), true
}

// NormalizeIcebergCatalogDDL widens Doris `TINYINT` and `SMALLINT` column types to `INT`
// in `CREATE TABLE` and `ALTER TABLE` statements targeting `<icebergCatalog>.<db>.<table>`,
// because Apache Iceberg has no 8-bit or 16-bit integer primitive types.
func NormalizeIcebergCatalogDDL(stmt, icebergCatalog string) string {
	cleanCatalog := strings.Trim(strings.TrimSpace(icebergCatalog), "`")
	if cleanCatalog == "" {
		cleanCatalog = defaultIcebergCatalog
	}
	p, ok := skipSQLTrivia(stmt, 0)
	if !ok {
		return stmt
	}
	w1, next, ok := readSQLWord(stmt, p)
	if !ok {
		return stmt
	}
	p, ok = skipSQLTrivia(stmt, next)
	if !ok {
		return stmt
	}
	w2, next, ok := readSQLWord(stmt, p)
	if !ok || !strings.EqualFold(w2, "TABLE") {
		return stmt
	}

	switch {
	case strings.EqualFold(w1, "CREATE"):
		p, ok = skipSQLTrivia(stmt, next)
		if !ok {
			return stmt
		}
		if wIf, afterIf, ifOK := readSQLWord(stmt, p); ifOK && strings.EqualFold(wIf, "IF") {
			pNot, okNot := skipSQLTrivia(stmt, afterIf)
			if !okNot {
				return stmt
			}
			wNot, afterNot, notOK := readSQLWord(stmt, pNot)
			if !notOK || !strings.EqualFold(wNot, "NOT") {
				return stmt
			}
			pEx, okEx := skipSQLTrivia(stmt, afterNot)
			if !okEx {
				return stmt
			}
			wEx, afterEx, exOK := readSQLWord(stmt, pEx)
			if !exOK || !strings.EqualFold(wEx, "EXISTS") {
				return stmt
			}
			p = afterEx
		}
		cat, afterRef, refOK := readThreePartTableRef(stmt, p)
		if !refOK || !strings.EqualFold(cat, cleanCatalog) {
			return stmt
		}
		openParen, ok := skipSQLTrivia(stmt, afterRef)
		if !ok || openParen >= len(stmt) || stmt[openParen] != '(' {
			return stmt
		}
		closeParen, ok := findMatchingCloseParen(stmt, openParen)
		if !ok {
			return stmt
		}
		inner := stmt[openParen+1 : closeParen]
		rewrittenInner, changed := rewriteIcebergColumnDefinitionsBlock(inner)
		if !changed {
			return stmt
		}
		return stmt[:openParen+1] + rewrittenInner + stmt[closeParen:]

	case strings.EqualFold(w1, "ALTER"):
		p, ok = skipSQLTrivia(stmt, next)
		if !ok {
			return stmt
		}
		if wIf, afterIf, ifOK := readSQLWord(stmt, p); ifOK && strings.EqualFold(wIf, "IF") {
			pEx, okEx := skipSQLTrivia(stmt, afterIf)
			if !okEx {
				return stmt
			}
			wEx, afterEx, exOK := readSQLWord(stmt, pEx)
			if !exOK || !strings.EqualFold(wEx, "EXISTS") {
				return stmt
			}
			p = afterEx
		}
		cat, afterRef, refOK := readThreePartTableRef(stmt, p)
		if !refOK || !strings.EqualFold(cat, cleanCatalog) {
			return stmt
		}
		tailEnd := findTopLevelSemicolonOrEnd(stmt, afterRef)
		tail := stmt[afterRef:tailEnd]
		spans, ok := splitTopLevelSQLItemSpans(tail)
		if !ok {
			return stmt
		}
		var repls []spanReplacement
		for _, sp := range spans {
			clause := tail[sp.start:sp.end]
			rewrittenClause, changed := rewriteIcebergAlterClause(clause)
			if changed {
				repls = append(repls, spanReplacement{
					start: afterRef + sp.start,
					end:   afterRef + sp.end,
					text:  rewrittenClause,
				})
			}
		}
		if len(repls) == 0 {
			return stmt
		}
		return applySpanReplacements(stmt, repls)
	default:
		return stmt
	}
}

func findTopLevelSemicolonOrEnd(s string, start int) int {
	n := len(s)
	depth := 0
	for i := start; i < n; {
		nextTrivia, ok := skipSQLTrivia(s, i)
		if !ok {
			return n
		}
		if nextTrivia > i {
			i = nextTrivia
			continue
		}
		ch := s[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(s, i)
			if !ok {
				return n
			}
			i = nextQuote
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ';':
			if depth == 0 {
				return i
			}
		}
		i++
	}
	return n
}

func rewriteIcebergAlterClause(clause string) (string, bool) {
	p, ok := skipSQLTrivia(clause, 0)
	if !ok {
		return clause, false
	}
	action, next, ok := readSQLWord(clause, p)
	if !ok || (!strings.EqualFold(action, "ADD") && !strings.EqualFold(action, "MODIFY")) {
		return clause, false
	}
	p, ok = skipSQLTrivia(clause, next)
	if !ok || p >= len(clause) {
		return clause, false
	}
	if clause[p] != '`' && clause[p] != '(' {
		if w, afterW, wOK := readSQLWord(clause, p); wOK {
			switch strings.ToUpper(w) {
			case "COLUMN":
				p, ok = skipSQLTrivia(clause, afterW)
				if !ok || p >= len(clause) {
					return clause, false
				}
			case "INDEX", "KEY", "PARTITION", "CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN":
				return clause, false
			}
		}
	}
	if strings.EqualFold(action, "ADD") && clause[p] == '(' {
		closeParen, ok := findMatchingCloseParen(clause, p)
		if !ok {
			return clause, false
		}
		rewrittenInner, changed := rewriteIcebergColumnDefinitionsBlock(clause[p+1 : closeParen])
		if !changed {
			return clause, false
		}
		return clause[:p+1] + rewrittenInner + clause[closeParen:], true
	}
	typeStart, ok := skipSQLIdentToken(clause, p)
	if !ok || typeStart >= len(clause) {
		return clause, false
	}
	rewrittenSpec, changed := widenIcebergIntTypesInSpec(clause[typeStart:])
	if !changed {
		return clause, false
	}
	return clause[:typeStart] + rewrittenSpec, true
}

// IsCreateOrAlterViewStatement reports whether stmt is a CREATE [OR REPLACE] VIEW
// or ALTER VIEW statement.
func IsCreateOrAlterViewStatement(stmt string) bool {
	p, ok := skipSQLTrivia(stmt, 0)
	if !ok {
		return false
	}
	w1, next, ok := readSQLWord(stmt, p)
	if !ok {
		return false
	}
	p, ok = skipSQLTrivia(stmt, next)
	if !ok {
		return false
	}
	w2, next, ok := readSQLWord(stmt, p)
	if !ok {
		return false
	}
	if strings.EqualFold(w1, "ALTER") {
		return strings.EqualFold(w2, "VIEW")
	}
	if !strings.EqualFold(w1, "CREATE") {
		return false
	}
	if strings.EqualFold(w2, "VIEW") {
		return true
	}
	if !strings.EqualFold(w2, "OR") {
		return false
	}
	p, ok = skipSQLTrivia(stmt, next)
	if !ok {
		return false
	}
	w3, next, ok := readSQLWord(stmt, p)
	if !ok || !strings.EqualFold(w3, "REPLACE") {
		return false
	}
	p, ok = skipSQLTrivia(stmt, next)
	if !ok {
		return false
	}
	w4, _, ok := readSQLWord(stmt, p)
	return ok && strings.EqualFold(w4, "VIEW")
}

func prevNonSpaceByte(s string, pos int) byte {
	for i := pos - 1; i >= 0; i-- {
		if !isSQLSpace(s[i]) {
			return s[i]
		}
	}
	return 0
}

func trimLeadingSQLTrivia(s string) string {
	p, ok := skipSQLTrivia(s, 0)
	if !ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(s[p:])
}

func hasUnquotedLineComment(s string) bool {
	n := len(s)
	for i := 0; i < n; {
		ch := s[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(s, i)
			if !ok {
				return true
			}
			i = nextQuote
			continue
		}
		if ch == '/' && i+1 < n && s[i+1] == '*' {
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return true
			}
			i += 2 + end + 2
			continue
		}
		if (ch == '-' && i+1 < n && s[i+1] == '-') || ch == '#' {
			return true
		}
		i++
	}
	return false
}

func encodeCompositeOrderField(expr string) string {
	wrapped := expr
	if hasUnquotedLineComment(expr) {
		wrapped = expr + "\n"
	}
	return fmt.Sprintf(
		"IF((%s) IS NULL, '0:', CONCAT('1:', IF(CAST((%s) AS STRING) REGEXP '^[0-9]+$', LPAD(CAST((%s) AS STRING), 20, '0'), CAST((%s) AS STRING))))",
		wrapped, wrapped, wrapped, wrapped,
	)
}

func rewriteOrderKeyStructs(s string) (string, bool) {
	n := len(s)
	var repls []spanReplacement
	for i := 0; i < n; {
		nextTrivia, ok := skipSQLTrivia(s, i)
		if !ok {
			return s, false
		}
		if nextTrivia > i {
			i = nextTrivia
			continue
		}
		ch := s[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(s, i)
			if !ok {
				return s, false
			}
			i = nextQuote
			continue
		}
		if isSQLIdentStart(ch) && (i == 0 || !isSQLIdentPart(s[i-1])) && prevNonSpaceByte(s, i) != '.' {
			word, wordEnd, _ := readSQLWord(s, i)
			if strings.EqualFold(word, "MAX_BY") || strings.EqualFold(word, "MIN_BY") {
				openParen, ok := skipSQLTrivia(s, wordEnd)
				if ok && openParen < n && s[openParen] == '(' {
					if closeParen, ok := findMatchingCloseParen(s, openParen); ok {
						i = closeParen + 1
						continue
					}
				}
			}
			if strings.EqualFold(word, "STRUCT") {
				openParen, ok := skipSQLTrivia(s, wordEnd)
				if !ok {
					return s, false
				}
				if openParen < n && s[openParen] == '(' {
					closeParen, ok := findMatchingCloseParen(s, openParen)
					if !ok {
						return s, false
					}
					fields, ok := splitTopLevelSQLItems(s[openParen+1 : closeParen])
					if ok && len(fields) > 0 {
						encoded := make([]string, 0, len(fields))
						validFields := true
						for _, rawField := range fields {
							cleanField := trimLeadingSQLTrivia(rawField)
							if cleanField == "" {
								validFields = false
								break
							}
							rewrittenField, _ := rewriteOrderKeyStructs(cleanField)
							encoded = append(encoded, encodeCompositeOrderField(rewrittenField))
						}
						if validFields {
							repls = append(repls, spanReplacement{
								start: i,
								end:   closeParen + 1,
								text:  "CONCAT_WS(CHAR(1), " + strings.Join(encoded, ", ") + ")",
							})
							i = closeParen + 1
							continue
						}
					}
					i = closeParen + 1
					continue
				}
			}
			i = wordEnd
			continue
		}
		i++
	}
	if len(repls) == 0 {
		return s, false
	}
	return applySpanReplacements(s, repls), true
}

// NormalizeNereidsViewOrderKeys rewrites composite `STRUCT(e_1, ..., e_k)` order-key
// expressions inside the second argument of `MAX_BY(valExpr, ordExpr)` and `MIN_BY(valExpr, ordExpr)`
// into an order-preserving lexicographic `CONCAT_WS(CHAR(1), ...)` key accepted by Doris 4.1.5 Nereids.
func NormalizeNereidsViewOrderKeys(stmt string) string {
	n := len(stmt)
	var repls []spanReplacement
	for i := 0; i < n; {
		nextTrivia, ok := skipSQLTrivia(stmt, i)
		if !ok {
			return stmt
		}
		if nextTrivia > i {
			i = nextTrivia
			continue
		}
		ch := stmt[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(stmt, i)
			if !ok {
				return stmt
			}
			i = nextQuote
			continue
		}
		if isSQLIdentStart(ch) && (i == 0 || !isSQLIdentPart(stmt[i-1])) && prevNonSpaceByte(stmt, i) != '.' {
			word, wordEnd, _ := readSQLWord(stmt, i)
			if strings.EqualFold(word, "MAX_BY") || strings.EqualFold(word, "MIN_BY") {
				openParen, ok := skipSQLTrivia(stmt, wordEnd)
				if !ok {
					return stmt
				}
				if openParen < n && stmt[openParen] == '(' {
					closeParen, ok := findMatchingCloseParen(stmt, openParen)
					if !ok {
						return stmt
					}
					inner := stmt[openParen+1 : closeParen]
					args, ok := splitTopLevelSQLItems(inner)
					if ok && len(args) == 2 {
						newVal := NormalizeNereidsViewOrderKeys(args[0])
						newOrd := NormalizeNereidsViewOrderKeys(args[1])
						newOrd, _ = rewriteOrderKeyStructs(newOrd)
						if newVal != args[0] || newOrd != args[1] {
							repls = append(repls, spanReplacement{
								start: openParen + 1,
								end:   closeParen,
								text:  newVal + "," + newOrd,
							})
						}
					}
					i = closeParen + 1
					continue
				}
			}
			i = wordEnd
			continue
		}
		i++
	}
	if len(repls) == 0 {
		return stmt
	}
	return applySpanReplacements(stmt, repls)
}

func isCreateTableStatement(stmt string) bool {
	p, ok := skipSQLTrivia(stmt, 0)
	if !ok {
		return false
	}
	w1, next, ok := readSQLWord(stmt, p)
	if !ok || !strings.EqualFold(w1, "CREATE") {
		return false
	}
	p, ok = skipSQLTrivia(stmt, next)
	if !ok {
		return false
	}
	w2, _, ok := readSQLWord(stmt, p)
	return ok && strings.EqualFold(w2, "TABLE")
}

func isSQLSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n'
}

func isSQLIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isSQLIdentPart(ch byte) bool {
	return isSQLIdentStart(ch) || (ch >= '0' && ch <= '9') || ch == '$'
}

func skipSQLTrivia(s string, pos int) (int, bool) {
	n := len(s)
	for pos < n {
		ch := s[pos]
		if isSQLSpace(ch) {
			pos++
			continue
		}
		if ch == '-' && pos+1 < n && s[pos+1] == '-' {
			pos += 2
			for pos < n && s[pos] != '\n' {
				pos++
			}
			continue
		}
		if ch == '#' {
			pos++
			for pos < n && s[pos] != '\n' {
				pos++
			}
			continue
		}
		if ch == '/' && pos+1 < n && s[pos+1] == '*' {
			end := strings.Index(s[pos+2:], "*/")
			if end < 0 {
				return pos, false
			}
			pos += 2 + end + 2
			continue
		}
		break
	}
	return pos, true
}

func skipSQLQuotedOrBacktick(s string, pos int) (int, bool) {
	n := len(s)
	if pos >= n {
		return pos, false
	}
	quote := s[pos]
	for i := pos + 1; i < n; {
		c := s[i]
		if c == '\\' && quote != '`' && i+1 < n {
			i += 2
			continue
		}
		i++
		if c == quote {
			if i < n && s[i] == quote {
				i++
				continue
			}
			return i, true
		}
	}
	return pos, false
}

func readSQLWord(s string, pos int) (string, int, bool) {
	if pos >= len(s) || !isSQLIdentStart(s[pos]) {
		return "", pos, false
	}
	end := pos + 1
	for end < len(s) && isSQLIdentPart(s[end]) {
		end++
	}
	return s[pos:end], end, true
}

func readSQLQuotedString(s string, start int) (string, int, bool) {
	if start >= len(s) || (s[start] != '"' && s[start] != '\'') {
		return "", start, false
	}
	quote := s[start]
	var b strings.Builder
	for i := start + 1; i < len(s); {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			b.WriteByte(s[i+1])
			i += 2
			continue
		}
		if c == quote {
			if i+1 < len(s) && s[i+1] == quote {
				b.WriteByte(quote)
				i += 2
				continue
			}
			return b.String(), i + 1, true
		}
		b.WriteByte(c)
		i++
	}
	return "", start, false
}

func findMatchingCloseParen(s string, openParenIdx int) (int, bool) {
	n := len(s)
	if openParenIdx >= n || s[openParenIdx] != '(' {
		return 0, false
	}
	depth := 1
	for i := openParenIdx + 1; i < n; {
		nextTrivia, ok := skipSQLTrivia(s, i)
		if !ok {
			return 0, false
		}
		if nextTrivia > i {
			i = nextTrivia
			continue
		}
		ch := s[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(s, i)
			if !ok {
				return 0, false
			}
			i = nextQuote
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
		}
		i++
	}
	return 0, false
}

func splitTopLevelNormalizedComma(s string) ([]string, bool) {
	n := len(s)
	var parts []string
	itemStart := 0
	for i := 0; i < n; {
		nextTrivia, ok := skipSQLTrivia(s, i)
		if !ok {
			return nil, false
		}
		if nextTrivia > i {
			i = nextTrivia
			continue
		}
		ch := s[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			nextQuote, ok := skipSQLQuotedOrBacktick(s, i)
			if !ok {
				return nil, false
			}
			i = nextQuote
			continue
		}
		if ch == ',' {
			parts = append(parts, s[itemStart:i])
			itemStart = i + 1
		}
		i++
	}
	parts = append(parts, s[itemStart:])
	return parts, true
}

func parsePropertyKeyValue(seg string) (string, bool) {
	p, ok := skipSQLTrivia(seg, 0)
	if !ok {
		return "", false
	}
	key, next, ok := readSQLQuotedString(seg, p)
	if !ok {
		return "", false
	}
	p, ok = skipSQLTrivia(seg, next)
	if !ok || p >= len(seg) || seg[p] != '=' {
		return "", false
	}
	p, ok = skipSQLTrivia(seg, p+1)
	if !ok || p >= len(seg) {
		return "", false
	}
	if seg[p] == '"' || seg[p] == '\'' {
		_, next, ok = readSQLQuotedString(seg, p)
		if !ok {
			return "", false
		}
	} else {
		next = p
		for next < len(seg) && ((seg[next] >= 'a' && seg[next] <= 'z') || (seg[next] >= 'A' && seg[next] <= 'Z') || (seg[next] >= '0' && seg[next] <= '9') || seg[next] == '_' || seg[next] == '-' || seg[next] == '.') {
			next++
		}
		if next == p {
			return "", false
		}
	}
	p, ok = skipSQLTrivia(seg, next)
	if !ok || p != len(seg) {
		return "", false
	}
	return key, true
}

func extractMultilineIndents(inner string, segments []string) (string, string) {
	itemIndent := "    "
	for _, seg := range segments {
		firstNonWS := 0
		for firstNonWS < len(seg) && isSQLSpace(seg[firstNonWS]) {
			firstNonWS++
		}
		leading := seg[:firstNonWS]
		if nl := strings.LastIndexByte(leading, '\n'); nl >= 0 {
			itemIndent = leading[nl+1:]
			break
		}
	}
	lastNonWS := len(inner) - 1
	for lastNonWS >= 0 && isSQLSpace(inner[lastNonWS]) {
		lastNonWS--
	}
	closeIndent := ""
	if lastNonWS+1 <= len(inner) {
		trailing := inner[lastNonWS+1:]
		if nl := strings.LastIndexByte(trailing, '\n'); nl >= 0 {
			closeIndent = trailing[nl+1:]
		}
	}
	return itemIndent, closeIndent
}

func stripCoupledPropsInner(inner string) (string, bool, bool) {
	segments, ok := splitTopLevelNormalizedComma(inner)
	if !ok || len(segments) == 0 {
		return inner, false, false
	}
	kept := make([]string, 0, len(segments))
	strippedCount := 0
	for _, seg := range segments {
		key, ok := parsePropertyKeyValue(seg)
		if !ok {
			return inner, false, false
		}
		if isCoupledModeDisallowedProp(key) {
			strippedCount++
			continue
		}
		kept = append(kept, strings.TrimSpace(seg))
	}
	if strippedCount == 0 {
		return inner, false, false
	}
	if len(kept) == 0 {
		return "", true, true
	}
	if !strings.Contains(inner, "\n") {
		return strings.Join(kept, ", "), false, true
	}
	itemIndent, closeIndent := extractMultilineIndents(inner, segments)
	rewritten := "\n" + itemIndent + strings.Join(kept, ",\n"+itemIndent) + "\n" + closeIndent
	return rewritten, false, true
}
