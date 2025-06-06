# Web Scraping Ethics and Best Practices

## 🎯 Learning Objectives

After reading this guide, you will understand:
1. Legal and ethical considerations for web scraping
2. Technical best practices for respectful scraping
3. How to avoid getting blocked or banned
4. Alternative approaches to scraping (APIs, partnerships)
5. Specific considerations for New Zealand racing sites

## ⚖️ Legal and Ethical Framework

### 1. Legal Considerations

**✅ Generally Legal:**
- Scraping publicly available information
- Respecting robots.txt files
- Reasonable request rates
- Personal research and learning
- Non-commercial educational use

**❌ Potentially Problematic:**
- Ignoring robots.txt or Terms of Service
- Overwhelming servers with requests
- Scraping copyrighted content for commercial use
- Accessing password-protected areas
- Circumventing anti-scraping measures

**🚨 Definitely Illegal:**
- Unauthorized access to private systems
- Copyright infringement
- Data theft or privacy violations
- Interfering with website operation

### 2. New Zealand Specific Considerations

**Privacy Act 2020:**
- Be careful with personal information
- Racing data often includes jockey/trainer names (public figures)
- Horse ownership information may be sensitive

**Copyright Considerations:**
- TAB.nz owns their data presentation
- Racing results may be factual (not copyrightable)
- Website design and unique content is protected

## 🤖 Technical Best Practices

### 1. Respect robots.txt

```python
# Always check robots.txt first
# https://www.tab.co.nz/robots.txt

from urllib.robotparser import RobotFileParser

def check_robots_txt(base_url, user_agent):
    robots_url = f"{base_url}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    return rp.can_fetch(user_agent, base_url)
```

### 2. Implement Rate Limiting

```python
import time
import random

class RespectfulScraper:
    def __init__(self, min_delay=1.0, max_delay=3.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        
    def polite_request(self, url):
        # Random delay to appear more human
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)
        
        # Make request with appropriate headers
        response = requests.get(url, headers={
            'User-Agent': 'Educational-Research-Bot/1.0 (contact@example.com)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        return response
```

### 3. Handle Errors Gracefully

```python
import requests
from requests.exceptions import RequestException

def robust_scraping(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response
            
        except RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Failed to fetch {url} after {max_retries} attempts")
                return None
```

## 🔍 Identifying Better Alternatives

### 1. Look for Official APIs First

**Research These Sources:**
- **Racing NZ API**: Check https://www.racing.nz for developer APIs
- **TAB API**: Look for official data feeds or partnerships
- **Government Data**: Search https://data.govt.nz for racing datasets
- **International APIs**: Study Hong Kong Jockey Club's API structure

### 2. RSS Feeds and Data Exports

```bash
# Check these URLs (many sites have hidden feeds):
curl -I https://www.tab.co.nz/rss
curl -I https://www.tab.co.nz/feeds/racing
curl -I https://www.racing.nz/api/results
```

### 3. Partnerships and Data Licensing

Consider reaching out to:
- Racing NZ for educational partnerships
- TAB for student research access
- University programs with existing data agreements

## 🛡️ Avoiding Detection and Blocks

### 1. Use Realistic User Agents

```python
# Good: Identifies your bot clearly
USER_AGENT = "Educational-Racing-Research/1.0 (student@university.edu)"

# Bad: Trying to hide as a real browser (can be seen as deceptive)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
```

### 2. Rotate IP Addresses (If Necessary)

```python
# For large-scale scraping, consider:
# - Cloud proxies (ensure they're legitimate)
# - Multiple server locations
# - Residential proxy services (paid, legal ones)

# For learning: Single IP with respectful delays is usually fine
```

### 3. Session Management

```python
import requests

# Use sessions for connection pooling and cookie management
session = requests.Session()
session.headers.update({
    'User-Agent': 'Educational-Research-Bot/1.0'
})

# Reuse the session for multiple requests
response1 = session.get('https://example.com/page1')
response2 = session.get('https://example.com/page2')
```

## 📊 TAB.nz Specific Research

### 1. Initial Reconnaissance

**TODO for Student: Research TAB.nz Structure**

1. **Visit TAB.nz manually:**
   - Study the racing section layout
   - Note URL patterns for different pages
   - Check mobile vs desktop versions

2. **Developer Tools Investigation:**
   ```javascript
   // In browser console, look for:
   // 1. AJAX calls to APIs
   console.log('Check Network tab for XHR requests');
   
   // 2. JavaScript data objects
   window.racingData || window.__INITIAL_STATE__ || window.APP_DATA
   
   // 3. Hidden data attributes
   document.querySelectorAll('[data-racing]')
   ```

3. **Check for APIs:**
   ```bash
   # Look for these common API endpoints:
   curl -I https://www.tab.co.nz/api/racing/today
   curl -I https://www.tab.co.nz/api/results
   curl -I https://api.tab.co.nz/racing
   ```

### 2. Rate Limiting Research

**Determine TAB's Limits:**
- Start with 1 request per 5 seconds
- Monitor for any rate limiting responses (429 status)
- Check if they use Cloudflare or similar protection
- Look for rate limit headers in responses

### 3. Data Structure Analysis

**HTML Structure to Find:**
```html
<!-- Look for patterns like: -->
<div class="race-card" data-race-id="...">
  <span class="race-time">14:30</span>
  <span class="track-name">Ellerslie</span>
  <div class="runners">
    <div class="runner" data-horse="...">
      <span class="horse-name">Thunder Bay</span>
      <span class="jockey">J. McDonald</span>
      <span class="odds">$3.20</span>
    </div>
  </div>
</div>
```

## 🎓 Educational Scraping Framework

### 1. Learning-Focused Approach

```python
class EducationalScraper:
    """
    A scraper designed for learning, not production.
    Emphasizes understanding over efficiency.
    """
    
    def __init__(self):
        self.request_count = 0
        self.errors = []
        self.discoveries = []
        
    def learning_request(self, url):
        """Make a request with educational logging."""
        print(f"📡 Request #{self.request_count + 1}: {url}")
        
        # Check robots.txt
        if not self.check_robots_permission(url):
            print("🚫 robots.txt disallows this request")
            return None
            
        # Make request with delays
        time.sleep(2)  # Always be respectful
        
        try:
            response = requests.get(url, timeout=10)
            self.request_count += 1
            
            print(f"✅ Response: {response.status_code} ({len(response.text)} chars)")
            return response
            
        except Exception as e:
            self.errors.append(str(e))
            print(f"❌ Error: {e}")
            return None
```

### 2. Research Documentation

**Keep a Research Log:**
```markdown
# TAB.nz Scraping Research Log

## Date: 2024-01-15
### Findings:
- robots.txt allows: /racing/today
- Rate limit appears to be: ~20 requests/minute
- Racing data structure: CSS class "race-card"
- Mobile site has different selectors

### Next Steps:
- [ ] Test different URL patterns
- [ ] Map out complete CSS selector structure  
- [ ] Check for mobile API endpoints
- [ ] Contact TAB about educational access
```

## 🚨 Warning Signs to Avoid

### 1. Aggressive Scraping Patterns

**Don't Do This:**
```python
# BAD: Overwhelming the server
for url in urls:
    response = requests.get(url)  # No delays
    process_immediately(response)  # No rate limiting
```

**Do This Instead:**
```python
# GOOD: Respectful scraping
for url in urls:
    time.sleep(random.uniform(2, 5))  # Random delays
    response = polite_request(url)    # With error handling
    if response:
        process_response(response)
```

### 2. Circumventing Protections

**Avoid:**
- Disabling JavaScript detection
- Bypassing CAPTCHAs
- Using stolen cookies or sessions
- Rotating user agents to evade detection

**Instead:**
- Respect anti-bot measures
- Look for official API alternatives
- Contact site owners for permission
- Use public data sources when available

## 📋 Pre-Scraping Checklist

Before writing any scraping code:

- [ ] ✅ Read the target website's Terms of Service
- [ ] ✅ Check robots.txt for permissions
- [ ] ✅ Look for official APIs or data feeds
- [ ] ✅ Research rate limiting and technical constraints
- [ ] ✅ Plan respectful request patterns (delays, headers)
- [ ] ✅ Implement error handling and recovery
- [ ] ✅ Set up monitoring for your own impact
- [ ] ✅ Have a backup plan if scraping fails
- [ ] ✅ Document your research and findings
- [ ] ✅ Consider reaching out to site owners

## 🎯 Next Steps for Your Project

### 1. Immediate Actions

1. **Research Phase** (Week 1):
   - Manual exploration of TAB.nz
   - robots.txt analysis
   - API endpoint discovery
   - Rate limit testing

2. **Implementation Phase** (Week 2):
   - Build respectful scraper framework
   - Implement CSS selector mapping
   - Add comprehensive error handling
   - Create data validation

3. **Optimization Phase** (Week 3):
   - Performance tuning
   - Monitoring implementation
   - Alternative data source integration
   - Documentation completion

### 2. Long-term Considerations

- **Scale Responsibly**: Start small, monitor impact
- **Build Relationships**: Consider contacting Racing NZ
- **Contribute Back**: Share educational findings
- **Stay Updated**: Monitor for API releases or policy changes

## 📚 Additional Resources

### Legal Resources
- [New Zealand Privacy Act 2020](https://privacy.org.nz/privacy-act-2020/)
- [Copyright Act 1994](https://www.legislation.govt.nz/act/public/1994/0143/latest/DLM345634.html)
- [Web Scraping Legal Guidelines](https://blog.apify.com/is-web-scraping-legal/)

### Technical Resources
- [Polite Web Scraping](https://docs.python-requests.org/en/latest/)
- [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [Scrapy Best Practices](https://docs.scrapy.org/en/latest/topics/practices.html)

### Racing Industry Resources
- [Racing NZ Official](https://www.racing.nz/)
- [International Racing Data Standards](https://www.horseracingintfed.com/)
- [Academic Racing Research](https://scholar.google.com/scholar?q=horse+racing+data+analysis)

---

**Remember**: The goal is to learn ethical scraping practices while building a useful racing data pipeline. When in doubt, err on the side of caution and respect for the websites you're accessing.

**Next Tutorial**: `03_prefect_concepts.py` - Learn workflow orchestration!