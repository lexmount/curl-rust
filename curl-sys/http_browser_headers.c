/*
 * Browser compatibility for HTTP/1 response fields containing bare CR.
 * Chromium's HttpUtil::AssembleRawHeaders splits on CR or LF, ignores empty
 * segments, and unfolds segments starting with SP/HTAB. Normalize before
 * http_rw_hd so framing headers and application callbacks see the same fields.
 * The physical LF still controls the header/body boundary in libcurl's parser.
 */
static CURLcode http_write_browser_segment(struct Curl_easy *data,
                                          struct dynbuf *line,
                                          size_t *emitted)
{
  size_t ignored;
  CURLcode result;
  if(!curlx_dyn_len(line))
    return CURLE_OK;
  result = curlx_dyn_addn(line, "\r\n", 2);
  if(!result) {
    result = http_rw_hd(data, curlx_dyn_ptr(line), curlx_dyn_len(line),
                        NULL, 0, &ignored);
    *emitted += curlx_dyn_len(line);
  }
  curlx_dyn_reset(line);
  return result;
}

static CURLcode http_rw_hd_browser(struct Curl_easy *data,
                                   const char *hd, size_t hdlen,
                                   const char *buf_remain, size_t blen,
                                   size_t *pconsumed)
{
  struct dynbuf line;
  CURLcode result = CURLE_OK;
  size_t len = hdlen;
  size_t emitted = 0;
  size_t offset = 0;
  bool continuable = FALSE;
  bool statusline = !data->req.headerline;

  if(len && hd[len - 1] == '\n')
    len--;
  if(len && hd[len - 1] == '\r')
    len--;
  if(!(data->conn->scheme->protocol & PROTO_FAMILY_HTTP) ||
     !memchr(hd, '\r', len))
    return http_rw_hd(data, hd, hdlen, buf_remain, blen, pconsumed);

  *pconsumed = 0;
  /* Retain strict NUL rejection even in a segment we would otherwise ignore. */
  if(memchr(hd, '\0', hdlen)) {
    failf(data, "NUL byte in header");
    return CURLE_WEIRD_SERVER_REPLY;
  }
  curlx_dyn_init(&line, CURL_MAX_HTTP_HEADER);
  while(offset < len) {
    const char *segment = hd + offset;
    const char *end = memchr(segment, '\r', len - offset);
    const char *colon;
    size_t slen = end ? (size_t)(end - segment) : len - offset;
    offset += slen + (end ? 1 : 0);
    if(!slen)
      continue;
    if(continuable && ISBLANK(segment[0])) {
      while(slen && ISBLANK(segment[0])) {
        segment++;
        slen--;
      }
      result = curlx_dyn_addn(&line, " ", 1);
      if(!result)
        result = curlx_dyn_addn(&line, segment, slen);
    }
    else {
      result = http_write_browser_segment(data, &line, &emitted);
      if(result)
        break;
      colon = memchr(segment, ':', slen);
      continuable = colon && colon != segment && !ISBLANK(segment[0]);
      /* Invalid field segments are ignored, as by HttpResponseHeaders. */
      if(statusline || continuable)
        result = curlx_dyn_addn(&line, segment, slen);
      statusline = FALSE;
    }
    if(result)
      break;
  }
  if(!result)
    result = http_write_browser_segment(data, &line, &emitted);
  curlx_dyn_free(&line);
  /* Do not let discarded segments bypass libcurl's aggregate header limit. */
  if(!result && hdlen > emitted)
    result = Curl_bump_headersize(data, hdlen - emitted, FALSE);
  return result;
}
