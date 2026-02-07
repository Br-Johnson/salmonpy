#!/usr/bin/env python3
import re

with open('deep_dives/results-network-graph.qmd', 'r') as f:
    content = f.read()

css = '''
/* Executive Summary Scorecard Styles */
.executive-scorecard {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 1.5rem;
  margin: 2rem 0;
  padding: 0;
}

.scorecard-card {
  background: white;
  border-radius: 8px;
  padding: 1.5rem;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  border-left: 4px solid;
  transition: transform 0.2s, box-shadow 0.2s;
}

.scorecard-card:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 8px rgba(0,0,0,0.15);
}

.scorecard-card.strong { border-left-color: #28a745; }
.scorecard-card.moderate { border-left-color: #ffc107; }
.scorecard-card.weak { border-left-color: #dc3545; }
.scorecard-card.neutral { border-left-color: #6c757d; }

.scorecard-value {
  font-size: 2.5rem;
  font-weight: bold;
  margin: 0.5rem 0;
  line-height: 1;
}

.scorecard-label {
  font-size: 0.9rem;
  color: #6c757d;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 0.5rem;
}

.scorecard-icon { font-size: 1.5rem; margin-bottom: 0.5rem; }

.scorecard-top-results {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid #e9ecef;
}

.scorecard-top-results-list {
  list-style: none;
  padding: 0;
  margin: 0.5rem 0 0 0;
}

.scorecard-top-results-list li {
  padding: 0.25rem 0;
  font-size: 0.85rem;
  color: #495057;
}

.scorecard-top-results-list li:before {
  content: "▸ ";
  color: #2f7ea0;
  font-weight: bold;
  margin-right: 0.5rem;
}
'''

content = content.replace('</style>', css + '\n</style>')

code = '''

## Executive Summary

```{ojs}
// Calculate executive summary metrics
executiveMetrics = {
  const rowsWithScores = rawRows.map(row => ({
    ...row,
    score: parseFloat(row.Score || 0)
  })).filter(row => row.score > 0);
  
  const totalScore = rowsWithScores.reduce((sum, row) => sum + row.score, 0);
  const uniqueLevel2 = new Set(rowsWithScores.map(row => row[COL_L2]).filter(Boolean));
  const numLevel2Results = uniqueLevel2.size;
  const avgScore = rowsWithScores.length > 0 ? totalScore / rowsWithScores.length : 0;
  
  const level2Scores = new Map();
  rowsWithScores.forEach(row => {
    const l2 = row[COL_L2];
    if (l2) {
      const current = level2Scores.get(l2) || 0;
      level2Scores.set(l2, current + row.score);
    }
  });
  
  const top3Results = Array.from(level2Scores.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([name, score]) => ({ name, score }));
  
  return {
    totalScore: Math.round(totalScore * 10) / 10,
    numLevel2Results,
    avgScore: Math.round(avgScore * 10) / 10,
    top3Results
  };
}
```

```{ojs}
function getScorecardClass(value, type) {
  if (type === 'score') {
    if (value >= 3.5) return 'strong';
    if (value >= 2.5) return 'moderate';
    if (value >= 1.5) return 'weak';
    return 'neutral';
  }
  if (type === 'count') {
    if (value >= 10) return 'strong';
    if (value >= 5) return 'moderate';
    return 'weak';
  }
  return 'neutral';
}

viewof executiveScorecard = {
  const metrics = executiveMetrics;
  const cards = [
    {
      icon: '📊',
      label: 'Total Contribution Score',
      value: metrics.totalScore.toFixed(1),
      class: getScorecardClass(metrics.totalScore / 50, 'score'),
      description: 'Sum of all program contributions'
    },
    {
      icon: '🎯',
      label: 'Results Supported',
      value: metrics.numLevel2Results.toString(),
      class: getScorecardClass(metrics.numLevel2Results, 'count'),
      description: 'Level 2 results with FADS contributions'
    },
    {
      icon: '⭐',
      label: 'Average Strength',
      value: metrics.avgScore.toFixed(1),
      class: getScorecardClass(metrics.avgScore, 'score'),
      description: 'Average contribution score per item'
    },
    {
      icon: '🏆',
      label: 'Top 3 Results',
      value: '',
      class: 'neutral',
      description: 'Highest contributing results',
      topResults: metrics.top3Results
    }
  ];
  
  const cardElements = cards.map(card => {
    const cardDiv = html`<div class="scorecard-card ${card.class}">
      <div class="scorecard-icon">${card.icon}</div>
      <div class="scorecard-label">${card.label}</div>
      ${card.value ? html`<div class="scorecard-value">${card.value}</div>` : ''}
      <div style="font-size: 0.8rem; color: #6c757d; margin-top: 0.5rem;">
        ${card.description}
      </div>
      ${card.topResults ? html`
        <div class="scorecard-top-results">
          <ul class="scorecard-top-results-list">
            ${card.topResults.map(result => 
              html`<li title="Total score: ${result.score.toFixed(1)}">
                ${result.name.length > 60 ? result.name.substring(0, 60) + '...' : result.name}
              </li>`
            )}
          </ul>
        </div>
      ` : ''}
    </div>`;
    return cardDiv;
  });
  
  return html`<div class="executive-scorecard">
    ${cardElements}
  </div>`;
}
```

```{ojs}
executiveScorecard
```
'''

pattern = r'(</style>\s*\n\n\n\n\n)'
content = re.sub(pattern, r'\1' + code + '\n\n', content)

with open('deep_dives/results-network-graph.qmd', 'w') as f:
    f.write(content)

print('Done!')
