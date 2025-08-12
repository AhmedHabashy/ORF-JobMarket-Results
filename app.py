from flask import Flask, render_template, jsonify, request
import pandas as pd
import json
from collections import Counter

app = Flask(__name__)

# --- Global Data Store ---
job_data = None

# --- Data Loading and Processing ---
def load_job_data():
    """Load and process the XLSX job data once."""
    global job_data
    if job_data is not None:
        return True
    try:
        df = pd.read_excel('job_data.xlsx')
        df['auto_score'] = pd.to_numeric(df['auto_score'], errors='coerce')
        df['manual_score'] = pd.to_numeric(df['manual_score'], errors='coerce')
        df.dropna(subset=['auto_score', 'manual_score'], inplace=True)
        # Safely parse the JSON string into a list of dictionaries
        df['Automatability_Analysis_Parsed'] = df['Automatability_Analysis'].apply(
            lambda x: json.loads(x) if pd.notna(x) and isinstance(x, str) and x.strip().startswith('[') else []
        )
        job_data = df
        print(f"Successfully loaded and processed {len(df)} job records.")
        return True
    except Exception as e:
        print(f"Error loading job data: {e}")
        return False

# --- Page Rendering Routes ---
@app.route('/')
def analysis_dashboard():
    """Renders the main analysis dashboard page."""
    load_job_data()
    return render_template('analysis_dashboard.html')

@app.route('/explorer')
def job_explorer():
    """Renders the job explorer page."""
    load_job_data()
    return render_template('job_explorer.html')

# --- Shared API Endpoints (Used by both views) ---
@app.route('/api/stats')
def get_stats():
    """Gets overall statistics for the entire dataset."""
    if job_data is None: return jsonify({'error': 'No data available'}), 500
    
    sector_count = job_data['Sector'].nunique() if 'Sector' in job_data.columns else 0
    stats = {
        'total_jobs': len(job_data),
        'unique_level_4_categories': job_data['level_4_name'].nunique(),
        'sector_count': sector_count,
        'avg_auto_score': round(job_data['auto_score'].mean(), 1),
        'avg_manual_score': round(job_data['manual_score'].mean(), 1)
    }
    return jsonify(stats)

@app.route('/api/levels')
def get_available_levels():
    """Gets all available filtering levels and their category counts."""
    if job_data is None: return jsonify({'error': 'No data available'}), 500
    
    levels = []
    for i in range(1, 5):
        level_column = f'level_{i}_name'
        if level_column in job_data.columns:
            levels.append({
                'level': i,
                'name': f'Level {i}',
                'count': job_data[level_column].nunique()
            })
    return jsonify(levels)

@app.route('/api/categories')
def get_categories():
    """Gets a list of categories for a given level, sorted by job count."""
    if job_data is None: return jsonify({'error': 'No data available'}), 500
    
    level = request.args.get('level', '4')
    level_column = f'level_{level}_name'
    if level_column not in job_data.columns:
        return jsonify({'error': f'Level {level} not available'}), 400
    
    category_counts = job_data[level_column].value_counts()
    categories_with_counts = [
        {'name': category, 'count': int(count)}
        for category, count in category_counts.items()
    ]
    return jsonify(categories_with_counts)

@app.route('/api/level_chart_data')
def get_level_chart_data():
    """Gets data for the hierarchical bar charts."""
    if job_data is None: return jsonify({'error': 'No data available'}), 500

    try:
        level = int(request.args.get('level'))
        if not 1 <= level <= 4:
            return jsonify({'error': 'Invalid level specified'}), 400

        filtered_df = job_data.copy()

        # Filter by parent selections if they are provided
        for i in range(1, level):
            parent_level_key = f'level_{i}_name'
            parent_selection = request.args.get(parent_level_key)
            if parent_selection:
                filtered_df = filtered_df[filtered_df[parent_level_key] == parent_selection]
        
        target_level_col = f'level_{level}_name'
        if target_level_col not in filtered_df.columns or filtered_df[target_level_col].isnull().all():
            return jsonify([]) # Return empty list if no data for this level

        # Group by the target level, calculate avg score and job count
        grouped = filtered_df.groupby(target_level_col).agg(
            avg_score=('auto_score', 'mean'),
            job_count=('Job_Title', 'count')
        ).reset_index()

        # Clean up and sort the data
        grouped['avg_score'] = grouped['avg_score'].round(1)
        grouped.rename(columns={target_level_col: 'name'}, inplace=True)
        grouped.dropna(subset=['name'], inplace=True)
        grouped = grouped.sort_values(by='avg_score', ascending=False)
        
        return jsonify(grouped.to_dict('records'))

    except (ValueError, KeyError) as e:
        return jsonify({'error': f'Bad request or invalid parameter: {e}'}), 400
    except Exception as e:
        return jsonify({'error': f'An unexpected error occurred: {e}'}), 500

@app.route('/api/risk_distribution')
def get_risk_distribution():
    """Calculates risk distribution (low, medium, high) for a given filter."""
    if job_data is None: return jsonify({'error': 'No data available'}), 500
    
    category = request.args.get('category')
    level = request.args.get('level', '4')
    
    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]
    
    low_risk = len(filtered_data[filtered_data['auto_score'] < 30])
    medium_risk = len(filtered_data[(filtered_data['auto_score'] >= 30) & (filtered_data['auto_score'] < 60)])
    high_risk = len(filtered_data[filtered_data['auto_score'] >= 60])
    
    return jsonify({
        'low_risk': low_risk,
        'medium_risk': medium_risk,
        'high_risk': high_risk,
        'total': len(filtered_data)
    })

@app.route('/api/jobs')
def get_jobs():
    """
    Handles both fetching all jobs for charts and paginated jobs for the explorer.
    - `fetch_all=true`: Returns a simplified list of all jobs for charting.
    - Otherwise: Returns a paginated list of detailed jobs.
    """
    if job_data is None: return jsonify({'error': 'No data available'}), 500

    category = request.args.get('category')
    level = request.args.get('level', '4')
    fetch_all = request.args.get('fetch_all', 'false').lower() == 'true'

    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]

    if fetch_all:
        # Simplified response for charts
        return jsonify(filtered_data[['auto_score', 'manual_score']].to_dict('records'))

    # Paginated response for job explorer
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 4))
    
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_data = filtered_data.iloc[start_idx:end_idx]

    jobs_json = []
    for _, row in paginated_data.iterrows():
        job_dict = {
            'Job_Title': row['Job_Title'],
            'Job_Description': row['Job_Description'],
            f'level_{level}_name': row.get(f'level_{level}_name', ''),
            f'level_{level}_code': row.get(f'level_{level}_code', ''),
            'auto_score': round(row['auto_score'], 1),
            'manual_score': round(row['manual_score'], 1),
            'automatability_tasks': row['Automatability_Analysis_Parsed']
        }
        jobs_json.append(job_dict)

    category_stats = {
        'total_jobs': len(filtered_data),
        'avg_auto_score': round(filtered_data['auto_score'].mean(), 1) if not filtered_data.empty else 0,
        'avg_manual_score': round(filtered_data['manual_score'].mean(), 1) if not filtered_data.empty else 0
    }

    return jsonify({
        'jobs': jobs_json,
        'page': page,
        'per_page': per_page,
        'has_more': end_idx < len(filtered_data),
        'category_stats': category_stats
    })


# --- API Endpoints for Job Explorer View ---
@app.route('/api/job/<job_title>')
def get_job_detail(job_title):
    """Gets detailed information for a single job title."""
    if job_data is None: return jsonify({'error': 'No data available'}), 500
    
    job_row = job_data[job_data['Job_Title'] == job_title].iloc[0]
    if not job_row.empty:
        processed_tasks = []
        for task in job_row['Automatability_Analysis_Parsed']:
            processed_task = task.copy()
            processed_task['importance_classification'] = task.get('importance_classification', 'Not specified')
            processed_task['reasoning'] = task.get('reasoning', 'No reasoning provided')
            processed_tasks.append(processed_task)
        
        job_detail = {
            'Job_Title': job_row['Job_Title'],
            'Job_Description': job_row['Job_Description'],
            'level_1_name': job_row.get('level_1_name', ''),
            'level_2_name': job_row.get('level_2_name', ''),
            'level_3_name': job_row.get('level_3_name', ''),
            'level_4_name': job_row.get('level_4_name', ''),
            'level_4_code': job_row.get('level_4_code', ''),
            'auto_score': round(job_row['auto_score'], 1),
            'manual_score': round(job_row['manual_score'], 1),
            'automatability_tasks': processed_tasks
        }
        return jsonify(job_detail)
    else:
        return jsonify({'error': 'Job not found'}), 404

# --- API Endpoints for Analysis Dashboard View ---
@app.route('/api/task_analysis')
def get_task_analysis():
    """Gets comprehensive task analysis for a selected category/level."""
    if job_data is None: return jsonify({'error': 'No data available'}), 500

    category = request.args.get('category')
    level = request.args.get('level', '4')

    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]

    all_tasks = [task for task_list in filtered_data['Automatability_Analysis_Parsed'] for task in task_list]

    if not all_tasks:
        return jsonify({'total_tasks': 0, 'automation_status': {}, 'task_type_distribution': {}, 'automation_by_type': {}, 'automation_drivers': [], 'automation_barriers': []})

    tasks_df = pd.DataFrame(all_tasks)
    
    # 1. Automation Status & Task Type Distribution
    automatable_count = int((tasks_df['automatability_flag'] == 'Automatable').sum())
    type_counts = tasks_df['importance_classification'].str.lower().value_counts().to_dict()

    # 2. Automation by Task Type
    automation_by_type = {}
    for task_type in ['primary', 'secondary', 'ancillary']:
        type_df = tasks_df[tasks_df['importance_classification'].str.lower() == task_type]
        if not type_df.empty:
            automatable = (type_df['automatability_flag'] == 'Automatable').sum()
            total = len(type_df)
            automation_by_type[task_type] = {
                'automation_percentage': round((automatable / total) * 100, 1) if total > 0 else 0
            }

    # 3. Automation Drivers and Barriers
    def get_top_reasons(df):
        valid_questions = df['question'].dropna().apply(lambda x: x if isinstance(x, list) else [])
        all_reasons = [q.replace('_', ' ').title() for sublist in valid_questions for q in sublist]
        return [{'reason': r, 'count': c} for r, c in Counter(all_reasons).most_common(10)]

    top_drivers = get_top_reasons(tasks_df[tasks_df['automatability_flag'] == 'Automatable'])
    top_barriers = get_top_reasons(tasks_df[tasks_df['automatability_flag'] != 'Automatable'])

    return jsonify({
        'total_tasks': len(all_tasks), 'total_jobs': len(filtered_data),
        'automation_status': {'automatable': automatable_count, 'non_automatable': len(all_tasks) - automatable_count},
        'task_type_distribution': {'primary': type_counts.get('primary', 0), 'secondary': type_counts.get('secondary', 0), 'ancillary': type_counts.get('ancillary', 0)},
        'automation_by_type': automation_by_type,
        'automation_drivers': top_drivers, 'automation_barriers': top_barriers
    })

@app.route('/api/automation_matrix')
def get_automation_matrix():
    """Gets automation matrix data for the scatter plot analysis."""
    if job_data is None: return jsonify({'error': 'No data available'}), 500

    category = request.args.get('category')
    level = request.args.get('level', '4')

    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]

    results = []
    for level_4_cat, group in filtered_data.groupby('level_4_name'):
        all_tasks = [task for task_list in group['Automatability_Analysis_Parsed'] for task in task_list]
        if not all_tasks: continue

        tasks_df = pd.DataFrame(all_tasks)
        total_tasks = len(tasks_df)
        overall_auto_pct = (tasks_df['automatability_flag'] == 'Automatable').sum() / total_tasks * 100

        primary_tasks_df = tasks_df[tasks_df['importance_classification'].str.lower() == 'primary']
        primary_auto_pct = 0
        if not primary_tasks_df.empty:
            primary_auto_pct = (primary_tasks_df['automatability_flag'] == 'Automatable').sum() / len(primary_tasks_df) * 100

        quadrant = "lower_right" # Default: Transformative
        if overall_auto_pct < 50 and primary_auto_pct >= 50: quadrant = "upper_left"  # Niche
        elif overall_auto_pct >= 50 and primary_auto_pct >= 50: quadrant = "upper_right" # High Risk
        elif overall_auto_pct < 50 and primary_auto_pct < 50: quadrant = "lower_left"  # Safe

        results.append({
            'category': level_4_cat,
            'overall_automation_pct': round(overall_auto_pct, 1),
            'primary_automation_pct': round(primary_auto_pct, 1),
            'total_tasks': total_tasks, 'primary_tasks': len(primary_tasks_df),
            'total_jobs': len(group), 'quadrant': quadrant
        })
    return jsonify({'matrix_data': results})

# --- START: NEWLY ADDED ENDPOINT ---
@app.route('/api/hierarchy_tree_data')
def get_hierarchy_tree_data():
    """Builds and returns a nested JSON for the D3.js hierarchy chart."""
    if job_data is None:
        return jsonify({'error': 'No data available'}), 500

    try:
        # Group by all levels to get the stats for the leaf nodes (level 4)
        hierarchy_stats = job_data.groupby(
            ['level_1_name', 'level_2_name', 'level_3_name', 'level_4_name']
        ).agg(
            value=('auto_score', 'mean'),
            job_count=('Job_Title', 'count')
        ).reset_index()

        # Initialize the root of our tree
        root = {"name": "All Jobs", "children": []}
        
        # Use a dictionary to keep track of nodes to avoid duplicates and for quick lookup
        node_map = {}

        for _, row in hierarchy_stats.iterrows():
            parent_node = root
            
            # Build path from level 1 to 3
            path_levels = ['level_1_name', 'level_2_name', 'level_3_name']
            current_path = ""

            for level_col in path_levels:
                level_name = row[level_col]
                if pd.isna(level_name):
                    continue
                
                # Create a unique path key for the node map
                parent_path = current_path
                current_path = f"{parent_path}|{level_name}"

                # Find or create the node for the current level
                child_node = node_map.get(current_path)
                if not child_node:
                    child_node = {"name": level_name, "children": []}
                    node_map[current_path] = child_node
                    parent_node.get("children", []).append(child_node)
                
                parent_node = child_node

            # Add the leaf node (level 4)
            level_4_name = row['level_4_name']
            if pd.notna(level_4_name):
                leaf_node = {
                    "name": level_4_name,
                    "value": round(row['value'], 1),
                    "job_count": int(row['job_count'])
                }
                parent_node.get("children", []).append(leaf_node)

        # A recursive function to calculate parent averages from their children
        def calculate_parent_averages(node):
            if "children" not in node or not node["children"]:
                # This is a leaf node, return its values
                return node.get('value', 0), node.get('job_count', 0)
            
            total_weighted_score = 0
            total_jobs = 0
            
            # Recursively call for all children
            for child in node['children']:
                child_score, child_jobs = calculate_parent_averages(child)
                if child_jobs > 0:
                    total_weighted_score += child_score * child_jobs
                    total_jobs += child_jobs
            
            # Calculate and set the parent's values
            node['job_count'] = total_jobs
            node['value'] = round(total_weighted_score / total_jobs, 1) if total_jobs > 0 else 0
            
            return node['value'], node['job_count']

        # Start the recursive calculation from the root
        calculate_parent_averages(root)

        return jsonify(root)
        
    except Exception as e:
        print(f"Error building hierarchy tree: {e}")
        return jsonify({'error': f'An unexpected error occurred: {e}'}), 500
# --- END: NEWLY ADDED ENDPOINT ---


if __name__ == '__main__':
    app.run(host='0.0.0.0')