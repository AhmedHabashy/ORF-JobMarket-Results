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
    categories = job_data['level_4_name'].unique().tolist()
    return jsonify(sorted(categories))

@app.route('/api/jobs')
def get_jobs():
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500

    category = request.args.get('category')
    fetch_all = request.args.get('fetch_all') == 'true'

    filtered_data = job_data
    if category:
        filtered_data = job_data[job_data['level_4_name'] == category]

    # If fetching all for chart, don't paginate, return a simplified list
    if fetch_all:
        jobs_for_chart = filtered_data[['auto_score']].to_dict('records')
        return jsonify(jobs_for_chart)

    # --- Regular paginated request ---
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10)) # Changed to 10 per user request

    category_stats = {
        'total_jobs': len(filtered_data),
        'avg_auto_score': round(filtered_data['auto_score'].mean(), 2) if not filtered_data.empty else 0,
        'avg_manual_score': round(filtered_data['manual_score'].mean(), 2) if not filtered_data.empty else 0
    }

    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_data = filtered_data.iloc[start_idx:end_idx]

    jobs_json = []
    for _, row in paginated_data.iterrows():
        job_dict = {
            'Job_Title': row['Job_Title'],
            'Job_Description': row['Job_Description'],
            'level_4_name': row['level_4_name'],
            'level_4_code': row['level_4_code'],
            'auto_score': round(row['auto_score'], 2),
            'manual_score': round(row['manual_score'], 2),
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
        job_detail = {
            'Job_Title': row['Job_Title'],
            'Job_Description': row['Job_Description'],
            'level_4_name': row['level_4_name'],
            'level_4_code': row['level_4_code'],
            'auto_score': round(row['auto_score'], 2),
            'manual_score': round(row['manual_score'], 2),
            'automatability_tasks': row['Automatability_Analysis_Parsed']
        }
        return jsonify(job_detail)
    else:
        return jsonify({'error': 'Job not found'}), 404

@app.route('/api/stats')
def get_stats():
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    stats = {
        'total_jobs': len(job_data),
        'unique_level_4_categories': job_data['level_4_name'].nunique(),
        'avg_auto_score': round(job_data['auto_score'].mean(), 2),
        'avg_manual_score': round(job_data['manual_score'].mean(), 2)
    }
    return jsonify(stats)

if __name__ == '__main__':
    app.run(debug=True)