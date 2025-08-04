from flask import Flask, render_template, jsonify, request
import pandas as pd
import json

app = Flask(__name__)

# Global variable to store our data
job_data = None

def load_job_data():
    """Load and process the XLSX job data"""
    global job_data
    try:
        df = pd.read_excel('job_data.xlsx')
        df['auto_score'] = pd.to_numeric(df['auto_score'], errors='coerce')
        df['manual_score'] = pd.to_numeric(df['manual_score'], errors='coerce')
        df.dropna(subset=['auto_score', 'manual_score'], inplace=True)
        df['Automatability_Analysis_Parsed'] = df['Automatability_Analysis'].apply(
            lambda x: json.loads(x) if pd.notna(x) and isinstance(x, str) and x.strip().startswith('[') else []
        )
        job_data = df
        print(f"Successfully loaded {len(df)} job records")
        return True
    except Exception as e:
        print(f"Error loading job data: {e}")
        return False

@app.route('/')
def index():
    if job_data is None and not load_job_data():
        return "Error: Could not load job data."
    return render_template('index.html')

@app.route('/api/categories')
def get_categories():
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    
    level = request.args.get('level', '4')  # Default to level 4
    
    level_column = f'level_{level}_name'
    if level_column not in job_data.columns:
        return jsonify({'error': f'Level {level} not available'}), 400
    
    # Get categories with their job counts and sort by frequency (descending)
    category_counts = job_data[level_column].value_counts().to_dict()
    categories_with_counts = [
        {
            'name': category,
            'count': count
        }
        for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
    ]
    
    return jsonify(categories_with_counts)

@app.route('/api/levels')
def get_available_levels():
    """Get all available filtering levels"""
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    
    levels = []
    for i in range(1, 5):  # Check levels 1-4
        level_column = f'level_{i}_name'
        if level_column in job_data.columns:
            levels.append({
                'level': i,
                'name': f'Level {i}',
                'count': job_data[level_column].nunique()
            })
    
    return jsonify(levels)

@app.route('/api/jobs')
def get_jobs():
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500

    category = request.args.get('category')
    level = request.args.get('level', '4')
    fetch_all = request.args.get('fetch_all') == 'true'

    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]

    # If fetching all for chart, don't paginate, return a simplified list
    if fetch_all:
        jobs_for_chart = filtered_data[['auto_score']].to_dict('records')
        return jsonify(jobs_for_chart)

    # --- Regular paginated request ---
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 4))  # Changed to 4 per user request

    category_stats = {
        'total_jobs': len(filtered_data),
        'avg_auto_score': round(filtered_data['auto_score'].mean(), 1) if not filtered_data.empty else 0,
        'avg_manual_score': round(filtered_data['manual_score'].mean(), 1) if not filtered_data.empty else 0
    }

    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_data = filtered_data.iloc[start_idx:end_idx]

    jobs_json = []
    for _, row in paginated_data.iterrows():
        job_dict = {
            'Job_Title': row['Job_Title'],
            'Job_Description': row['Job_Description'],
            f'level_{level}_name': row[f'level_{level}_name'] if f'level_{level}_name' in row else '',
            f'level_{level}_code': row[f'level_{level}_code'] if f'level_{level}_code' in row else '',
            'auto_score': round(row['auto_score'], 1),
            'manual_score': round(row['manual_score'], 1),
            'automatability_tasks': row['Automatability_Analysis_Parsed']
        }
        jobs_json.append(job_dict)

    return jsonify({
        'jobs': jobs_json,
        'page': page,
        'per_page': per_page,
        'has_more': end_idx < len(filtered_data),
        'category_stats': category_stats
    })

@app.route('/api/job/<job_title>')
def get_job_detail(job_title):
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    
    job_row = job_data[job_data['Job_Title'] == job_title]
    if not job_row.empty:
        row = job_row.iloc[0]
        
        # Process automatability tasks to include importance_classification and reasoning
        processed_tasks = []
        for task in row['Automatability_Analysis_Parsed']:
            processed_task = task.copy()
            # Ensure importance_classification is included
            if 'importance_classification' not in processed_task:
                processed_task['importance_classification'] = 'Not specified'
            # Ensure reasoning is included
            if 'reasoning' not in processed_task:
                processed_task['reasoning'] = 'No reasoning provided'
            processed_tasks.append(processed_task)
        
        job_detail = {
            'Job_Title': row['Job_Title'],
            'Job_Description': row['Job_Description'],
            'level_1_name': row['level_1_name'] if 'level_1_name' in row else '',
            'level_2_name': row['level_2_name'] if 'level_2_name' in row else '',
            'level_3_name': row['level_3_name'] if 'level_3_name' in row else '',
            'level_4_name': row['level_4_name'] if 'level_4_name' in row else '',
            'level_4_code': row['level_4_code'] if 'level_4_code' in row else '',
            'auto_score': round(row['auto_score'], 1),
            'manual_score': round(row['manual_score'], 1),
            'automatability_tasks': processed_tasks
        }
        return jsonify(job_detail)
    else:
        return jsonify({'error': 'Job not found'}), 404

@app.route('/api/stats')
def get_stats():
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    
    # Calculate sector count
    sector_count = 0
    if 'Sector' in job_data.columns:
        sector_count = job_data['Sector'].nunique()
    
    stats = {
        'total_jobs': len(job_data),
        'unique_level_4_categories': job_data['level_4_name'].nunique(),
        'sector_count': sector_count,
        'avg_auto_score': round(job_data['auto_score'].mean(), 1),
        'avg_manual_score': round(job_data['manual_score'].mean(), 1)
    }
    return jsonify(stats)

@app.route('/api/risk_distribution')
def get_risk_distribution():
    """Get risk distribution data for donut chart"""
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    
    category = request.args.get('category')
    level = request.args.get('level', '4')
    
    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]
    
    # Calculate risk distribution
    low_risk = len(filtered_data[filtered_data['auto_score'] < 30])
    medium_risk = len(filtered_data[(filtered_data['auto_score'] >= 30) & (filtered_data['auto_score'] < 60)])
    high_risk = len(filtered_data[filtered_data['auto_score'] >= 60])
    
    return jsonify({
        'low_risk': low_risk,
        'medium_risk': medium_risk,
        'high_risk': high_risk,
        'total': len(filtered_data)
    })

if __name__ == '__main__':
    app.run(debug=True)